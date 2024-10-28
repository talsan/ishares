from datetime import datetime
from config import Aws, iShares
import pandas as pd
import argparse
import os
import logging
from ishares.utils import s3_helpers
from ishares import etf_downloader
import pandas_market_calendars as mcal

log = logging.getLogger(__name__)

# reference table for a given etf ticker with metadata, including
# the funds inception date so we know how far to go back to get holdings
etf_index = pd.read_csv(iShares.ETF_MASTER_INDEX_LOC).set_index('ticker').to_dict('index')

def get_trading_day_month_ends(exchange_code, start_date, end_date, output_format):
    nyse = mcal.get_calendar(exchange_code)
    trading_days_df = pd.DataFrame({'trading_date':
                                        nyse.valid_days(start_date=start_date,
                                                        end_date=end_date)
                                    })
    trading_days_df['yymm'] = trading_days_df['trading_date'].dt.strftime('%y%m')
    trading_day_month_ends = trading_days_df.groupby('yymm')['trading_date'].max(). \
        dt.strftime(output_format).to_list()

    return trading_day_month_ends


def build_holdings_download_queue(ticker: str, overwrite: bool = False) -> list:
    first_available_date = datetime.strptime(iShares.FIRST_AVAILABLE_HOLDINGS_DATE, '%Y-%m-%d')
    etf_start_date = datetime.strptime(etf_index[ticker]['start_date'], '%Y-%m-%d')

    request_start_date = max([first_available_date, etf_start_date])
    request_end_date = datetime.now().strftime('%Y-%m-%d')

    tradingday_month_ends = get_trading_day_month_ends('NYSE', request_start_date, request_end_date, '%Y-%m-%d')

    path_prefix = f'type=holdings/state=formatted/etf={ticker}'
    file_prefix = 'asofdate='

    if overwrite:
        holdings_download_list = tradingday_month_ends
    else:
        existing_download_dates = s3_helpers.list_keys(Bucket=Aws.S3_ETF_HOLDINGS_BUCKET,
                                                       Prefix=f'{path_prefix}/{file_prefix}',
                                                       full_path=False,
                                                       remove_ext=True)

        unprocessed_download_dates = sorted(list(set(tradingday_month_ends) - set(existing_download_dates)))
        holdings_download_list = unprocessed_download_dates

    log.info(f'queued {len(holdings_download_list)} holding dates for download')
    return holdings_download_list


def main(tickers: list, overwrite: bool) -> None:
    print(tickers)
    for ticker in tickers:
        holdings_download_list = build_holdings_download_queue(ticker, overwrite)
        for holding_date in holdings_download_list:
            try:
                etf_downloader.main(ticker, holding_date)
            except BaseException as e:
                log.error(e)


# everything below will only be executed if this script is called from the command line
# if this file is imported, nothing below will be executed
if __name__ == "__main__":
    # command line arguments
    parser = argparse.ArgumentParser(description='batch process that queues-and-invokes a series of etf_downloaders')
    parser.add_argument('tickers', help=f'list of ETF Tickers (space-delimeted) you wish to download; '
                                        f'full list of tickers located here: {iShares.ETF_MASTER_INDEX_LOC}', nargs='+')
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
    main(args.tickers,args.overwrite)
    log.info(f'successfully completed script')
