import requests
from datetime import datetime
import random
from config import Aws, iShares
import pandas as pd
import argparse
import os
from io import StringIO, BytesIO
import gzip
import shutil
import boto3
import logging
import json
import multiprocessing as mp

log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------------
# pre-loaded reference data (i.e. things we only want computed once)
# -----------------------------------------------------------------------------------
etf_index = pd.read_csv(iShares.ETF_MASTER_INDEX_LOC).set_index('ticker').to_dict('index')

with open(iShares.HOLDINGS_FILE_SCHEMAS, 'r') as f:
    holdings_file_schemas = json.load(f)


# HoldingsProcessor manages the data-pipeline from ishares.com to aws s3
class HoldingsProcessor:

    def __init__(self, ticker, holdings_date, outputpath):
        # etf-level information
        self.ticker = ticker
        self.name = etf_index[ticker]['name']
        self.start_date = datetime.strptime(etf_index[ticker]['start_date'], '%Y-%m-%d')
        self.product_url = etf_index[ticker]['product_url']

        # holdings information
        self.holdings_date = datetime.strptime(holdings_date, '%Y-%m-%d').strftime('%Y-%m-%d')
        self.response_content = bytes()
        self.holdings_df = pd.DataFrame()
        self.input_schema = ''  # detected via validate_input_schema() only after file is sourced

        # output info
        self.outputpath = outputpath
        self.key_raw = f'type=holdings/state=raw/etf={ticker}/asofdate={holdings_date}.gz'
        self.key_formatted = f'type=holdings/state=formatted/etf={ticker}/asofdate={holdings_date}.csv'
        self.filestr = ''

        log.info(f'HoldingsProcessor for {self.holdings_date} successfully initialized')

    def request_csv(self):
        yyyymmdd = datetime.strptime(self.holdings_date, '%Y-%m-%d').strftime('%Y%m%d')
        request = dict(url=f'{iShares.ROOT}{self.product_url}/{iShares.AJAX_REQUEST_CODE}',
                       params={'fileType': 'csv',
                               'fileName': f'{self.ticker}_holdings',
                               'dataType': 'fund',
                               'asOfDate': yyyymmdd},
                       headers={'User-Agent': random.choice(iShares.USER_AGENT_LIST)})

        response = requests.get(**request)

        # todo error handling / logging here
        assert response != 200, "response error (not 200)"
        assert len(response.content) > 0, "empty response"

        log.info(f'successful response from ishares.com for request: {response.url}')
        return response.content

    def get_holdings_df(self):
        self.response_content = self.request_csv()
        csv_buffer = StringIO(self.response_content.decode())
        self.holdings_df = pd.read_csv(csv_buffer, header=9, thousands=',', na_values='-').dropna(thresh=10)
        return self

    def archive_original_csv(self):

        input_file_buffer = BytesIO(self.response_content)
        compressed_file_buffer = BytesIO()

        if self.outputpath == 's3':
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
            log.info(f'archive raw to s3 success: {s3_output_url}')

        else:
            output_path = f'{self.outputpath.rstrip("/")}/{self.key_raw}'
            if not os.path.exists(os.path.dirname(output_path)):
                os.makedirs(os.path.dirname(output_path))
            with gzip.GzipFile(output_path, mode='wb') as gz_out:
                shutil.copyfileobj(input_file_buffer, gz_out)
            print(f'pid={mp.current_process().pid} wrote: {self.key_raw} locally to {self.outputpath}')

        return self

    def validate_input(self):
        # schema/columns validation
        schema_lookup = [k for k, v in holdings_file_schemas.items()
                         if v['columns'] == self.holdings_df.columns.to_list()]
        if len(schema_lookup) == 0:
            log.error(f'holdings_df columns: {self.holdings_df.columns.to_list()}')
            raise Exception(f'schema unrecognizable: not found in {iShares.HOLDINGS_FILE_SCHEMAS}')
        else:
            log.info(f'schema successfully detected: {schema_lookup[0]}')
            self.input_schema = schema_lookup[0]

        # holding-data/content validation
        if self.holdings_df.shape[0] < 5:
            log.error(self.holdings_df)
            raise Exception('less than 5 rows in the sourced holding file')

        return self

    def format_output(self):
        # column names
        new_columns = self.holdings_df.columns \
            .str.lower() \
            .str.replace('[^a-z_\s]', '') \
            .str.strip() \
            .str.replace('\s+', '_')
        column_map = dict(zip(self.holdings_df.columns, new_columns))
        self.holdings_df.rename(columns=column_map, inplace=True, errors='raise')

        # column contents
        self.holdings_df['weight'] /= 100
        self.holdings_df.insert(0, 'asofdate', self.holdings_date)
        log.info(f'file successfully formatted prior to s3 upload')
        return self

    def df_to_filestr(self):
        csv_buffer = StringIO()
        self.holdings_df.to_csv(csv_buffer, index=False, line_terminator='\n')
        self.filestr = csv_buffer.getvalue()
        return self

    def upload_filestr(self) -> None:

        if self.outputpath.lower() == 's3':
            aws_session = boto3.Session(aws_access_key_id=Aws.AWS_KEY,
                                        aws_secret_access_key=Aws.AWS_SECRET)
            s3 = aws_session.client('s3')
            s3.put_object(Body=self.filestr, Bucket=Aws.S3_ETF_HOLDINGS_BUCKET, Key=self.key_formatted)
            log.info(f'pid[{mp.current_process().pid}] wrote {self.key_formatted} to s3')

        else:
            full_outputpath = f'{self.outputpath.rstrip("/")}/{self.key_formatted}'
            if not os.path.exists(os.path.dirname(full_outputpath)):
                os.makedirs(os.path.dirname(full_outputpath))
            with open(full_outputpath, 'w') as f:
                f.write(self.filestr)
            log.info(f'pid[{mp.current_process().pid}] wrote locally to: ./data/{self.key_formatted}')


def main(ticker: str, holdings_date: str, outputpath: str) -> pd.DataFrame:
    holdings = HoldingsProcessor(ticker, holdings_date, outputpath)
    holdings.get_holdings_df() \
        .archive_original_csv() \
        .validate_input() \
        .format_output() \
        .df_to_filestr() \
        .upload_filestr()

    return holdings.holdings_df


# everything below will only be executed if this script is called from the command line
# if this file is imported, nothing below will be executed
if __name__ == "__main__":
    # command line arguments
    parser = argparse.ArgumentParser(description='Extract-Transform-Load individual iShares ETF holdings for a given ticker and date ')
    parser.add_argument('ticker', help=f'etf ticker you wish to download; '
                                       f'full list of tickers located here: {iShares.ETF_MASTER_INDEX_LOC}')
    parser.add_argument('holdings_date', help='YYYY-MM-DD; must be month-end-trading dates')
    parser.add_argument('outputpath', help=f'where to send output on local machine; if outputpath==\'s3\', output is '
                                           f'uploaded to the Aws.OUPUT_BUCKET variable defined in config.py')
    args = parser.parse_args()

    # logging (will inherit log calls from utils.pricing and utils.s3_helpers)
    this_file = os.path.basename(__file__).replace('.py', '')
    log_id = f'{this_file}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    logging.basicConfig(filename=f'./logs/{log_id}.log', level=logging.INFO,
                        format=f'%(asctime)s - %(name)s - %(levelname)s - {args.ticker} - %(message)s')

    # run main
    main(args.ticker, args.holdings_date, args.outputpath)
    log.info(f'successfully completed script')
