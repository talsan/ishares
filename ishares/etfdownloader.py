import requests
from ishares.utils import s3_helpers, pricing
from datetime import datetime
import random
import config
import pandas as pd
import argparse
import os
import time
from io import StringIO, BytesIO
import gzip
import shutil
import boto3
import logging
import json

log = logging.getLogger(__name__)

'''
Overview: 
    For a given iShares ETF ticker, this script: 
        1) Determines which holding dates need to be downloaded (with file update/overwrite optionality)
            via: build_holdings_download_list(ticker, overwrite=False)
        2) Downloads the csv, formats it, and uploads it to aws s3
            via HoldingsProcessor object

Intended Usage:
    1) As a script, running on a scheduler or manually.   
        Example:
            c:/../ishares>python etfdownloader.py IWV --overwrite
        
    2)  If needed, can also operate as a module, to run one-off processing.
        !!! IF YOU RUN THIS OVER A LOOP REMEMBER TO SLEEP BETWEEN REQUESTS !!! 
        Example:
            import etfdownloader
            etfdownloader.process_holdings('IEFA','2020-01-30', return_holdings_df=True, s3_upload=False)

Input Parameters (command line arguments): 
    ticker -- ETF ticker you wish to download
    overwrite -- Overwrite holdings that have already been downloaded to aws s3
    
Output: 
    aws s3 files:
        path = see config.iShares
    
'''

# -----------------------------------------------------------------------------------
# pre-loaded reference data (i.e. things we only want computed once)
# -----------------------------------------------------------------------------------
etf_index = pd.read_csv(config.iShares.ETF_MASTER_INDEX_LOC).set_index('ticker').to_dict('index')
with open(config.iShares.HOLDINGS_FILE_SCHEMAS, 'r') as f:
    holdings_file_schemas = json.load(f)

# -----------------------------------------------------------------------------------
# misc configurations
# -----------------------------------------------------------------------------------
aws_session = boto3.Session(aws_access_key_id=config.Access.AWS_KEY,
                            aws_secret_access_key=config.Access.AWS_SECRET)


# HoldingsProcessor manages the data-pipeline from ishares.com to aws s3
class HoldingsProcessor:

    def __init__(self, ticker, holdings_date):
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

        # s3 info
        self.s3_client = aws_session.client('s3', region_name=config.iShares.S3_REGION_NAME)
        self.s3_upload_response = ''  # returned from aws after s3-upload

        log.info(f'HoldingsProcessor for {self.holdings_date} successfully initialized')

    def request_csv(self):
        yyyymmdd = datetime.strptime(self.holdings_date, '%Y-%m-%d').strftime('%Y%m%d')
        request = dict(url=f'{config.iShares.ROOT}{self.product_url}/{config.iShares.AJAX_REQUEST_CODE}',
                       params={'fileType': 'csv',
                               'fileName': f'{self.ticker}_holdings',
                               'dataType': 'fund',
                               'asOfDate': yyyymmdd},
                       headers={'User-Agent': random.choice(config.USER_AGENT_LIST)})

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
        s3_key = f'type=holdings/state=raw/etf={self.ticker}/asofdate={self.holdings_date}.gz'

        # gzip file
        # code adapted from https://gist.github.com/tobywf/079b36898d39eeb1824977c6c2f6d51e
        # with explanation here https://tobywf.com/2017/06/gzip-compression-for-boto3/
        input_file_buffer = BytesIO(self.response_content)
        compressed_file_buffer = BytesIO()
        with gzip.GzipFile(fileobj=compressed_file_buffer, mode='wb') as gz:
            shutil.copyfileobj(input_file_buffer, gz)
        compressed_file_buffer.seek(0)

        self.s3_upload_response = self.s3_client.upload_fileobj(Bucket=config.iShares.S3_ETF_HOLDINGS_BUCKET,
                                                                Key=s3_key,
                                                                Fileobj=compressed_file_buffer,
                                                                ExtraArgs={'ContentType': 'text/csv',
                                                                           'ContentEncoding': 'gzip'})

        s3_output_url = f'{config.iShares.S3_OBJECT_ROOT}/{config.iShares.S3_ETF_HOLDINGS_BUCKET}/{s3_key}'
        log.info(f'archive raw to s3 success: {s3_output_url}')

        return self

    def validate_input(self):
        # schema/columns validation
        schema_lookup = [k for k, v in holdings_file_schemas.items()
                         if v['columns'] == self.holdings_df.columns.to_list()]
        if len(schema_lookup) == 0:
            log.error(f'holdings_df columns: {self.holdings_df.columns.to_list()}')
            raise Exception(f'schema unrecognizable: not found in {config.iShares.HOLDINGS_FILE_SCHEMAS}')
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

    def put_holdings_in_s3(self):
        s3_key = f'type=holdings/state=formatted/etf={self.ticker}/asofdate={self.holdings_date}.csv'
        csv_buffer = StringIO()
        self.holdings_df.to_csv(csv_buffer, index=False)
        self.s3_upload_response = self.s3_client.put_object(Bucket=config.iShares.S3_ETF_HOLDINGS_BUCKET,
                                                            Key=s3_key,
                                                            Body=csv_buffer.getvalue())
        s3_output_url = f'{config.iShares.S3_OBJECT_ROOT}/{config.iShares.S3_ETF_HOLDINGS_BUCKET}/{s3_key}'
        log.info(f's3 upload success: {s3_output_url}')
        return self


def process_holdings(ticker, holdings_date, return_holdings_df=False, s3_upload=True):
    holdings = HoldingsProcessor(ticker, holdings_date)
    holdings.get_holdings_df() \
        .archive_original_csv() \
        .validate_input() \
        .format_output()

    if s3_upload:
        holdings.put_holdings_in_s3()

    if return_holdings_df:
        return holdings.holdings_df


def build_holdings_download_list(ticker, overwrite=False):
    first_available_date = datetime.strptime(config.iShares.FIRST_AVAILABLE_HOLDINGS_DATE, '%Y-%m-%d')
    etf_start_date = datetime.strptime(etf_index[ticker]['start_date'], '%Y-%m-%d')

    tradingday_month_ends = pricing.get_tradingday_monthends(ticker='IBM',
                                                             start_date=config.iShares.FIRST_AVAILABLE_HOLDINGS_DATE)

    all_possible_dates = [datetime.strftime(tme, '%Y-%m-%d')
                          for tme in tradingday_month_ends
                          if (tme > etf_start_date) and (tme >= first_available_date)]

    if overwrite:
        holdings_download_list = all_possible_dates
    else:
        existing_download_dates = s3_helpers.list_keys(Bucket=config.iShares.S3_ETF_HOLDINGS_BUCKET,
                                                       Prefix=f'type=holdings/state=formatted/etf={ticker}/asofdate=',
                                                       full_path=False,
                                                       remove_ext=True)
        unprocessed_download_dates = sorted(list(set(all_possible_dates) - set(existing_download_dates)))
        holdings_download_list = unprocessed_download_dates

    log.info(f'queued {len(holdings_download_list)} holding dates for download')
    return holdings_download_list


def sleep_between_requests():
    sleep_seconds = random.randint(config.iShares.MIN_SLEEP_BETWEEN_REQUESTS,
                                   config.iShares.MAX_SLEEP_BETWEEN_REQUESTS)
    log.info(f'post request sleep for {sleep_seconds} seconds ...')
    time.sleep(sleep_seconds)


def main(ticker, overwrite):
    holdings_download_list = build_holdings_download_list(ticker, overwrite)
    for holding_date in holdings_download_list:
        process_holdings(ticker, holding_date)
        sleep_between_requests()


# everything below will only be executed if this script is called from the command line
# if this file is imported, nothing below will be executed
if __name__ == "__main__":
    # command line arguments
    parser = argparse.ArgumentParser(description='Extract Raw ETF Holding Files')
    parser.add_argument('ticker', help=f'ETF ticker you wish to download')
    parser.add_argument('--overwrite', help=f'Overwrite holdings that have already been downloaded to S3',
                        action='store_true')
    args = parser.parse_args()

    # logging (will inherit log calls from utils.pricing and utils.s3_helpers)
    this_file = os.path.basename(__file__).replace('.py', '')
    log_id = f'{this_file}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    logging.basicConfig(filename=f'./logs/{log_id}.log', level=logging.INFO,
                        format=f'%(asctime)s - %(name)s - %(levelname)s - {args.ticker} - %(message)s')

    # run main
    main(args.ticker, args.overwrite)
    log.info(f'successfully completed script')
