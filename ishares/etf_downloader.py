from datetime import datetime
from config import Aws, iShares, ScrapingBee
from scrapingbee import ScrapingBeeClient
import pandas as pd
import argparse
import os
import json
from io import StringIO, BytesIO
import gzip
import shutil
import boto3
import multiprocessing as mp

# -----------------------------------------------------------------------------------
# pre-loaded data
# -----------------------------------------------------------------------------------
etf_index = pd.read_csv(iShares.ETF_MASTER_INDEX_LOC).set_index('ticker').to_dict('index')

# HoldingsProcessor manages the data-pipeline from ishares.com to aws s3
class HoldingsProcessor:

    def __init__(self, ticker, holdings_date):

        # etf-index
        self.etf_index = pd.read_csv(iShares.ETF_MASTER_INDEX_LOC).set_index('ticker').to_dict('index')

        # etf-level information
        self.ticker = ticker
        self.name = self.etf_index[ticker]['name']
        self.start_date = datetime.strptime(self.etf_index[ticker]['start_date'], '%Y-%m-%d')
        self.product_url = self.etf_index[ticker]['product_url']

        # request information
        self.holdings_date = datetime.strptime(holdings_date, '%Y-%m-%d').strftime('%Y-%m-%d')
        self.request_url = ''

        # response information
        self.response_content = bytes()
        self.response_json = dict()
        self.formatted_json = list()
        self.holdings_df = pd.DataFrame()

        # output information
        self.key_raw = f'type=holdings/state=raw/etf={ticker}/asofdate={holdings_date}.gz'
        self.key_formatted = f'type=holdings/state=formatted/etf={ticker}/asofdate={holdings_date}.csv'
        self.filestr = str()

        print(f'HoldingsProcessor for {self.ticker} + {self.holdings_date} successfully initialized')

    def request_holdings(self):

        yyyymmdd = datetime.strptime(self.holdings_date, '%Y-%m-%d').strftime('%Y%m%d')
        self.request_url = f'{iShares.ROOT}/' \
                           f'{self.product_url}/1467271812596.ajax?fileType=json&tab=all&asOfDate={yyyymmdd}'
        print(f'requesting: {self.request_url}')

        sb_client = ScrapingBeeClient(os.environ.get('SCRAPINGBEE_KEY'))
        def sb_request_w_retry(request_url, timeout, max_retries):
            for i in range(max_retries):
                try:
                    sb_response = sb_client.get(request_url, params={'render_js': 'False'}, timeout=timeout)
                    return sb_response
                except BaseException as err:
                    print(f'err: {err} on attempt {i + 1}')
                    continue
            return None

        response = sb_request_w_retry(self.request_url,
                                      timeout=ScrapingBee.timeout,
                                      max_retries=ScrapingBee.max_retries)

        assert response is not None, f'ScrapingBee client likely timed out (after {ScrapingBee.max_retries} attempts)'
        assert response != 200, f'{response.status_code}:{response.text}'
        assert len(response.content) > 10, 'empty response'

        print(f'response snippet: {response.content[0:200]}')

        self.response_content = response.content
        return self

    def archive_response(self):
        input_file_buffer = BytesIO(self.response_content)
        compressed_file_buffer = BytesIO()

        aws_session = boto3.Session(aws_access_key_id=Aws.AWS_KEY,
                                    aws_secret_access_key=Aws.AWS_SECRET)
        s3 = aws_session.client('s3')

        with gzip.GzipFile(fileobj=compressed_file_buffer, mode='wb') as gz:
            shutil.copyfileobj(input_file_buffer, gz)
        compressed_file_buffer.seek(0)

        s3.upload_fileobj(Bucket=Aws.S3_ETF_HOLDINGS_BUCKET,
                          Key=self.key_raw,
                          Fileobj=compressed_file_buffer,
                          ExtraArgs={'ContentType': 'text/plain',
                                     'ContentEncoding': 'gzip'})

        s3_output_url = f'{Aws.S3_OBJECT_ROOT}/{Aws.S3_ETF_HOLDINGS_BUCKET}/{self.key_raw}'
        print(f'archive raw to s3 success: {s3_output_url}')

        return self

    def map_raw_item(self, unmapped_item):
        return {
            'ticker': unmapped_item[0],
            'name': unmapped_item[1],
            'sector': unmapped_item[2],
            'asset_class': unmapped_item[3],
            'market_value': unmapped_item[4]['raw'],
            'weight': unmapped_item[5]['raw'],
            'notional_value': unmapped_item[6]['raw'],
            'shares': unmapped_item[7]['raw'],
            'cusip': unmapped_item[8],
            'isin': unmapped_item[9],
            'sedol': unmapped_item[10],
            'price': unmapped_item[11]['raw'],
            'location': unmapped_item[12],
            'exchange': unmapped_item[13],
            'currency': unmapped_item[14],
            'fx_rate': unmapped_item[15],
            'maturity': unmapped_item[16]
        }

    def holdings_raw_to_json(self):
        self.response_json = json.loads(self.response_content)
        input_items = self.response_json['aaData']
        for input_item in input_items:
            mapped_item = self.map_raw_item(input_item)
            self.formatted_json.append(mapped_item)
        return self

    def holdings_json_to_df(self):
        self.holdings_df = pd.DataFrame(self.formatted_json)
        assert self.holdings_df.shape[0] > 0, 'err: 0 holdings in formatted response'
        self.holdings_df['weight'] /= 100
        self.holdings_df.insert(1, 'etf', self.ticker)
        self.holdings_df.insert(2, 'holdings_date', self.holdings_date)
        return self

    def df_to_filestr(self):
        csv_buffer = StringIO()
        self.holdings_df.to_csv(csv_buffer, index=False, lineterminator='\n')
        self.filestr = csv_buffer.getvalue()
        return self

    def upload_filestr(self) -> None:
        aws_session = boto3.Session(aws_access_key_id=Aws.AWS_KEY,
                                    aws_secret_access_key=Aws.AWS_SECRET)
        s3 = aws_session.client('s3')
        s3.put_object(Body=self.filestr, Bucket=Aws.S3_ETF_HOLDINGS_BUCKET, Key=self.key_formatted)
        print(f'pid[{mp.current_process().pid}] wrote {self.key_formatted} to s3')


# function for one-off request for holdings (e.g. when importing this module in a different process)
def get_etf_holdings_df(ticker: str, holdings_date) -> pd.DataFrame:
    holdings = HoldingsProcessor(ticker, holdings_date)
    holdings.request_holdings() \
        .holdings_raw_to_json() \
        .holdings_json_to_df()
    return holdings.holdings_df


def main(ticker: str, holdings_date: str) -> HoldingsProcessor:
    holdings = HoldingsProcessor(ticker, holdings_date)
    holdings.request_holdings() \
        .archive_response() \
        .holdings_raw_to_json() \
        .holdings_json_to_df() \
        .df_to_filestr() \
        .upload_filestr()

def lambda_handler(event, context):
    event_dict = {'etf': 'IWV', 'holdings_date': '2023-01-31'}
    event_json = json.dumps(event_dict)

    print(f'event: {event_json}')
    try:
        main(ticker=event_dict['etf'], holdings_date=event_dict['holdings_date'])
        out_msg = {
            'statusCode': 200,
            'body': f'successfully wrote {event}'
        }
        print(out_msg)
        return out_msg
    except Exception as e:
        print(str(e))
        raise e

# everything below will only be executed if this script is called from the command line
# if this file is imported, nothing below will be executed
if __name__ == "__main__":
    # command line arguments
    parser = argparse.ArgumentParser(
        description='Extract-Transform-Load individual iShares ETF holdings for a given ticker and date ')
    parser.add_argument('ticker', help=f'etf ticker you wish to download; '
                                       f'full list of tickers located here: {iShares.ETF_MASTER_INDEX_LOC}')
    parser.add_argument('holdings_date', help='YYYY-MM-DD; must be month-end-trading dates')
    args = parser.parse_args()

    # run main
    main(args.ticker, args.holdings_date, args.outputpath)

