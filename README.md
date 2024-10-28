# Scrape, Store, and Query iShares ETF holding files
Before (or after) diving into the code, if you want to understand the context behind this project, see [`READMEQuant.md`](https://github.com/talsan/ishares/blob/master/README_Quant.md)

## Quickly Get Monthly Holdings for an iShares ETF
- Run this script: [`./ishares/easy_downloader.py`](https://github.com/talsan/ishares/blob/master/ishares/easy_downloader.py)
#### Inputs
```pyton
etf_landing_page = 'https://www.ishares.com/us/products/239707/ishares-russell-1000-etf'
start_date = '2019-12-31'
end_date = datetime.now().strftime('%Y-%m-%d')
output_file = './data/direct_downloader/IWB_holdings_20220823.csv'
```
#### Output test
| as_of_date | ticker | name                           | sector                 | asset_class | market_value | weight  | notional_value | shares  | cusip     | isin         | sedol   | price   | location      | exchange                     | currency | fx_rate | maturity |
| ---------- | ------ | ------------------------------ | ---------------------- | ----------- | ------------ | ------- | -------------- | ------- | --------- | ------------ | ------- | ------- | ------------- | ---------------------------- | -------- | ------- | -------- |
| 2019-12-31 | AAPL   | APPLE INC                      | Information Technology | Equity      | 980033676.65 | 4.38868 | 980033676.65   | 3337421 | 037833100 | US0378331005 | 2046251 | 293.65  | United States | NASDAQ                       | USD      | 1.00    | -        |
| 2019-12-31 | MSFT   | MICROSOFT CORP                 | Information Technology | Equity      | 895545656.1  | 4.01033 | 895545656.1    | 5678793 | 594918104 | US5949181045 | 2588173 | 157.7   | United States | NASDAQ                       | USD      | 1.00    | -        |
| 2019-12-31 | AMZN   | AMAZON COM INC                 | Consumer Discretionary | Equity      | 573782037.6  | 2.56945 | 573782037.6    | 310515  | 023135106 | US0231351067 | 2000019 | 1847.84 | United States | NASDAQ                       | USD      | 1.00    | -        |
| 2019-12-31 | FB     | FACEBOOK CLASS A  INC          | Communication          | Equity      | 368197564.5  | 1.64882 | 368197564.5    | 1793898 | 30303M102 | US30303M1027 | B7TL820 | 205.25  | United States | NASDAQ                       | USD      | 1.00    | -        |
| 2019-12-31 | BRKB   | BERKSHIRE HATHAWAY INC CLASS B | Financials             | Equity      | 332579236.5  | 1.48932 | 332579236.5    | 1468341 | 084670702 | US0846707026 | 2073390 | 226.5   | United States | New York Stock Exchange Inc. | USD      | 1.00    | -        |
| 2019-12-31 | JPM    | JPMORGAN CHASE & CO            | Financials             | Equity      | 326360213.2  | 1.46147 | 326360213.2    | 2341178 | 46625H100 | US46625H1005 | 2190385 | 139.4   | United States | New York Stock Exchange Inc. | USD      | 1.00    | -        |

## Advanced Usage Patterns for Mass Downloading + Maintenance
#### Modules for getting etf holdings 
- `etf_downloader.py` provides functions to get individual etf holdings from ishares.com (wherein each call/event is a `etf_ticker + holding_date` pair).
#### Batch Processes for syncing iShares.com with your local or s3 filestores
- `sync_etf_downloader.py`invokes/queues a series of new events keeping local/cloud directories in sync with iShares.com
- Supports local or S3 file-store (see `outputpath` input parameter)
- Supports multiprocessing (see parameter in `config.py`)
- Polite, under-the-radar requests (various throttles and perameters available in `config.py`)

## Module Usage Examples
`./examples/ishares_to_df.py`
scrape a specific url:
```python
from ishares.etf_downloader import get_etf_holdings_df, get_etf_index

# index/metadata of all etfs on ishares product line
etf_index = get_etf_index()

# get holdings directly from iShares.com (R3000 on 2020-07-15)
df_ishares = get_etf_holdings_df(ticker='IWV',holdings_date='2020-07-15')
```
For more comprehensive data projects, you can run `sync_etf_downlaoder.py` for full holding histories (back to 2006) of multiple etfs. 
If you define `outputpath=='s3'` and [run the athena table ddl]('http://github.com'), you can run sql queries across different dates:
```python
from ishares.utils import s3_helpers, athena_helpers

# list available files in s3 bucket
etf_files = s3_helpers.list_keys(Bucket='etf-holdings')

# get a single file from s3
df_small = s3_helpers.get_etf_holdings('IWV', '2020-06-30')

# query a lot of data with Athena
df_big = athena_helpers.query('select * from qcdb.ishares_holdings '
                              'where etf=\'IWV\' '
                              'and '
                              'asofdate between date \'2020-01-31\' and date \'2020-06-30\'')
```

##### `sync_etf_downloader.py`
can be run anytime to keep etf holdings up to date. If `overwrite=True`, the entire etf holdings history is downloaded
##### Inputs: 
```
usage: batch_etf_downloader.py [-h] --outputpath OUTPUTPATH [--overwrite]
                               tickers [tickers ...]

batch ETL of iShares ETF holdings, supporting multiple funds and dates; option
to update or overwrite existing extracts

positional arguments:
  tickers               List of ETF Tickers (space-delimeted) you wish to
                        download; full list of tickers located here:
                        ./ishares/data/ishares-etf-index.csv

optional arguments:
  -h, --help            show this help message and exit
  --outputpath OUTPUTPATH
                        where to send output on local machine; if
                        outputpath=='s3', output is uploaded to the
                        Aws.OUPUT_BUCKET variable defined in config.py
  --overwrite           Overwrite holdings that have already been downloaded
                        to S3 (otherwise it's an update)

```

##### Output: 
S3 naming convention: `<config.Aws.OUPUT_BUCKET>/type=holdings/state=formatted/etf=IWF/asofdate=2006-09-29.csv`  
Local naming convention: [`./ishares/output/type=holdings/state=formatted/etf=IWF/asofdate=2006-09-29.csv`](https://github.com/talsan/ceopay/blob/master/data/masteridx/year%3D2020/qtr%3D2.txt)    
##### Archive of Original Extract (Pre-Transform-and-Load)
S3 naming convention: `<config.Aws.OUPUT_BUCKET>/type=holdings/state=formatted/etf=IWF/asofdate=2006-09-29.csv`  
Local naming convention: [`./ishares/output/type=holdings/state=formatted/etf=IWF/asofdate=2006-09-29.csv`](https://github.com/talsan/ceopay/blob/master/data/masteridx/year%3D2020/qtr%3D2.txt)  

##### Process Flow
1. `ishares/build_etf_master_index.py` - run this periodically to refresh the ETF universe
2. 
