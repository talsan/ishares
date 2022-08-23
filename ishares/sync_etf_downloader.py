from datetime import datetime
import random
from config import Aws, iShares
import pandas as pd
import argparse
import os
import time
import logging
import glob
import re
from ishares.utils import s3_helpers, pricing
from ishares import etf_downloader

log = logging.getLogger(__name__)

# reference table for a given etf ticker with metadata, including
# the funds inception date so we know how far to go back to get holdings
etf_index = pd.read_csv(iShares.ETF_MASTER_INDEX_LOC).set_index('ticker').to_dict('index')


def build_holdings_download_queue(ticker: str, outputpath: str, overwrite: bool = False) -> list:
    first_available_date = datetime.strptime(iShares.FIRST_AVAILABLE_HOLDINGS_DATE, '%Y-%m-%d')
    etf_start_date = datetime.strptime(etf_index[ticker]['start_date'], '%Y-%m-%d')

    tradingday_month_ends = pricing.get_tradingday_monthends(ticker='IBM',
                                                             start_date=iShares.FIRST_AVAILABLE_HOLDINGS_DATE)

    all_possible_dates = [datetime.strftime(tme, '%Y-%m-%d')
                          for tme in tradingday_month_ends
                          if (tme > etf_start_date) and (tme >= first_available_date)]

    path_prefix = f'type=holdings/state=formatted/etf={ticker}'
    file_prefix = 'asofdate='

    if overwrite:
        holdings_download_list = all_possible_dates
    else:
        if outputpath == 's3':
            existing_download_dates = s3_helpers.list_keys(Bucket=Aws.S3_ETF_HOLDINGS_BUCKET,
                                                           Prefix=f'{path_prefix}/{file_prefix}',
                                                           full_path=False,
                                                           remove_ext=True)
        else:
            existing_download_dates = [re.sub(file_prefix, '', re.sub('\.[^.]+$', '', os.path.basename(key)))
                                       for key in glob.glob(f'{outputpath.rstrip("/")}/**/*.csv', recursive=True)]

        unprocessed_download_dates = sorted(list(set(all_possible_dates) - set(existing_download_dates)))
        holdings_download_list = unprocessed_download_dates

    log.info(f'queued {len(holdings_download_list)} holding dates for download')
    return holdings_download_list


def sleep_between_requests() -> None:
    sleep_seconds = random.randint(iShares.MIN_SLEEP_BETWEEN_REQUESTS,
                                   iShares.MAX_SLEEP_BETWEEN_REQUESTS)
    log.info(f'post request sleep for {sleep_seconds} seconds ...')
    time.sleep(sleep_seconds)


def main(tickers: list, outputpath: str, overwrite: bool) -> None:
    print(tickers)
    for ticker in tickers:
        holdings_download_list = build_holdings_download_queue(ticker, outputpath, overwrite)
        for holding_date in holdings_download_list:
            try:
                etf_downloader.main(ticker, holding_date, outputpath)
            except BaseException as e:
                log.error(e)
            sleep_between_requests()


# everything below will only be executed if this script is called from the command line
# if this file is imported, nothing below will be executed
if __name__ == "__main__":
    # command line arguments
    parser = argparse.ArgumentParser(description='batch process that queues-and-invokes a series of etf_downloaders')
    parser.add_argument('tickers', help=f'list of ETF Tickers (space-delimeted) you wish to download; '
                                        f'full list of tickers located here: {iShares.ETF_MASTER_INDEX_LOC}', nargs='+')
    parser.add_argument('--outputpath', help=f'where to send output on local machine; if outputpath==\'s3\', output is '
                                             f'uploaded to the Aws.OUPUT_BUCKET variable defined in config.py',
                        required=True)
    parser.add_argument('--overwrite',
                        help=f'overwrite (re-download) etf holdings that have already been downloaded to <outputpath>; '
                             f'otherwise it\'s an update (i.e. only download new holdings files for a given ETF)',
                        action='store_true')
    args = parser.parse_args()

    # logging (will inherit log calls from utils.pricing and utils.s3_helpers)
    this_file = os.path.basename(__file__).replace('.py', '')
    log_id = f'{this_file}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    logging.basicConfig(filename=f'./logs/{log_id}.log', level=logging.INFO,
                        format=f'%(asctime)s - %(name)s - %(levelname)s - {args.tickers} - %(message)s')

    # run main
    main(args.tickers, args.outputpath, args.overwrite, )
    log.info(f'successfully completed script')
