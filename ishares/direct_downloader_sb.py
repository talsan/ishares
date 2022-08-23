import pandas as pd
import pandas_market_calendars as mcal
from datetime import datetime
import json
import os

# INPUTS
# ---------------------------------------------------------------------------------------------
# IWV from earliest available
etf_landing_page = 'https://www.ishares.com/us/products/239707/ishares-russell-1000-etf'
start_date = '2019-12-31'
end_date = datetime.now().strftime('%Y-%m-%d')
output_file = './data/direct_downloader/IWB_holdings_20220823.csv'
use_scraping_bee = False

# ---------------------------------------------------------------------------------------------

# month end trading days
# --------------------------------------------------------------------------------------------------
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


# --------------------------------------------------------------------------------------------------

# scrapingbee request module
# if not using scrabing bee, de-comment this section and use requests.get (instead of sb_request_w_retry)
# --------------------------------------------------------------------------------------------------
if use_scraping_bee:
    from dotenv import load_dotenv
    from scrapingbee import ScrapingBeeClient

    load_dotenv()
    sb_client = ScrapingBeeClient(os.environ.get('SCRAPINGBEE_KEY'))

    def sb_request_w_retry(request_url, timeout, max_retries):
        for i in range(max_retries):
            try:
                response = sb_client.get(request_url, params={'render_js': 'False'}, timeout=timeout)
                return response
            except BaseException as err:
                print(f'err: {err} on attempt {i + 1}')
                continue
        return None
else:
    import requests
# --------------------------------------------------------------------------------------------------

# json response from ishares is a list (not dict); list items need to be named/mapped
# map_raw_item() handles a single asset in the etf
# format_response() handles the entire response (looping through and mapping each asset)
# -------------------------------------------------------------------------------------------------
def map_raw_item(unmapped_item):
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


def format_response(response_json):
    input_items = response_json['aaData']
    output_items = []
    for input_item in input_items:
        mapped_item = map_raw_item(input_item)
        output_items.append(mapped_item)
    return (output_items)


# -------------------------------------------------------------------------------------------------


# main
# -------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    list_of_holdings = []
    yyyymmdd_queue = get_trading_day_month_ends('NYSE', start_date, end_date, '%Y%m%d')
    for yyyymmdd in yyyymmdd_queue:
        request_url = f'{etf_landing_page}/1467271812596.ajax?' \
                      f'fileType=json&tab=all&asOfDate={yyyymmdd}'
        print(f'requesting: {request_url}')

        if use_scraping_bee:
            response = sb_request_w_retry(request_url, timeout=10, max_retries=3)
        else:
            response = requests.get(request_url)

        if response is None:
            print(f'{"!" * 10}\nREQUEST FAILED({yyyymmdd}):\n'
                  f'ERROR: Request to SB Timed-Out\n'
                  f'{"!" * 10}')
        else:
            print(f'response.status_code: {response.status_code}')

            if response.status_code != 200:

                print(f'{"!" * 10}\nREQUEST FAILED({yyyymmdd}):\n'
                      f'status code = {response.status_code}\n'
                      f'response_message = {response.text}\n'
                      f'{"!" * 10}')
            else:
                response_json = json.loads(response.content)
                holdings_json = format_response(response_json)
                holdings_df = pd.DataFrame(holdings_json)

                if holdings_df.shape[0] == 0:
                    print(f'{"!" * 10}\nERROR: 0 ROWS ({yyyymmdd})\n{"!" * 10}')

                else:
                    print(f'number of rows: {holdings_df.shape[0]}')

                    # add date col
                    holdings_df.insert(loc=0,
                                       column='as_of_date',
                                       value=f'{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}')  # %Y-%m-%d
                    list_of_holdings.append(holdings_df)

    holdings = pd.concat(list_of_holdings)
    holdings.to_csv(output_file, index=False)
# -------------------------------------------------------------------------------------------------
