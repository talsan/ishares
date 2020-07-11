import requests
from lxml import html
import pandas as pd
import config

'''
Overview: 
This script scrapes the iShares landing page for their "universe" of ETFs. That source webpage (and resulting output) 
provides etf-level information (namely, inception dates and product page url's) required for downloading holding 
histories. 

Intended Usage:
As a script, intended to be run manually/periodically, if at all, since we currently don't need holdings for new 
iShares products). 

Input Parameters: 
    none
    
Output: 
    csv file:
        path = "./data/etf-master-index.json/" 

    output columns:
        ticker -- etf ticker
        name --  etf name
        start_date -- YYYY-MM-DD -- etf inception date, so we know how far to search for history
        product_url -- etf landing page (path to etf history of holdings files)
'''

def main():
    # source html document object
    r = requests.get(config.iShares.LANDING_PAGE)
    index_root = html.fromstring(r.text)

    # get table
    etf_table = index_root.xpath('//table[contains(.,"IWV")]')[0]

    # get table headers
    headers = etf_table.xpath('//thead/tr/th[@class="header-line"]/text()')

    # get desired content into a dataframe
    etf_info = pd.DataFrame({'ticker': etf_table.xpath(f'./tbody/tr/td[{headers.index("Ticker") + 1}]//text()'),
                             'name': etf_table.xpath(f'./tbody/tr/td[{headers.index("Name") + 1}]//text()'),
                             'start_date': etf_table.xpath(f'./tbody/tr/td[{headers.index("Incept. Date") + 1}]//text()'),
                             'product_url': etf_table.xpath('./tbody/tr/td[@class="links"][1]/a/@href')})

    # format dataframe start_date column to %Y%m%d
    etf_info['start_date'] = pd.to_datetime(etf_info['start_date'], format='%b %d, %Y')

    # local output
    etf_info.to_csv(config.iShares.ETF_MASTER_INDEX_LOC)
