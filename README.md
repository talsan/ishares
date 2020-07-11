# Data-Pipeline: iShares ETF holdings 
### Contents
1. Overview
2. Features
3. ETL Process Examples
4. Interactive Data Access

## Process Overview
1. ETL: Collect a full monthly history (back to 2006) of ETF holdings from iShares.com, for every ETF in the iShares product line  
2. Access: Persist data in S3 for direct access, and/or Athena for querying facilities

### Features & Options
#### Event-driven design:
- `etf_downloader.py` - a self-contained event for a given ticker & date
- `batch_etf_downloader.py` - a batch process that queues-and-invokes a series of etf-downloaders
#### AWS Integration: 
- Option to output locally or to S3
- If S3, Athena DDLs (`athenadll.sql`) are available for Table Creation, and simple boto3 wrappers (`athena_helpers`) for SQL Querying.
#### Careful Scraping
- Randomized sleep-between-request parameters configurable in `config.py`
- Randomized user-agents configurable in `config.py`

### Extract-Transform-Load (from iShares.com)
##### `etf_downloader.py`
self-contained script (and module) to process a single etf holding date
##### Inputs: 
```
Extract-Transform-Load individual iShares ETF holdings for a given ticker and
date

positional arguments:
  ticker         etf ticker you wish to download; full list of tickers located
                 here: ./data/ishares-etf-index.csv
  holdings_date  YYYY-MM-DD; must be month-end-trading dates
  outputpath     where to send output on local machine; if outputpath=='s3',
                 output is uploaded to the Aws.OUPUT_BUCKET variable defined
                 in config.py

optional arguments:
  -h, --help     show this help message and exit
```
##### Output: 
[Output Example](https://github.com/talsan/ceopay/blob/master/data/masteridx/year%3D2020/qtr%3D2.txt)  
S3 naming convention: `<config.Aws.OUPUT_BUCKET>/type=holdings/state=formatted/etf=IWF/asofdate=2006-09-29.csv`  
Local naming convention: `./ceopay/output/type=holdings/state=formatted/etf=IWF/asofdate=2006-09-29.csv`  
##### Archive of Original Extract (Pre-Transform-and-Load)
[Output Example](https://github.com/talsan/ceopay/blob/master/data/masteridx/year%3D2020/qtr%3D2.txt)  
S3 naming convention: `<config.Aws.OUPUT_BUCKET>/type=holdings/state=formatted/etf=IWF/asofdate=2006-09-29.csv`  
Local naming convention: `./ceopay/output/type=holdings/state=formatted/etf=IWF/asofdate=2006-09-29.csv`  

##### `batch_etf_downloader.py`
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

### Data Access Examples
```
from ishares.utils import s3_helpers, athena_helpers

# get a single file from s3
df_small = s3_helpers.get_etf_holdings('IWV', '2020-06-30')

# query a lot of data with Athena
df_big = athena_helpers.query('select * from qcdb.ishares_holdings '
                              'where '
                              'etf=\'IWV\' '
                              'and '
                              'asofdate between date \'2020-01-31\' and date \'2020-06-30\'')

# list available files in s3 bucket
etf_files = s3_helpers.list_keys(Bucket='etf-holdings')
```

### What's the use-case?
- iShares ETF holdings track well-defined indices - baskets of stocks that represent well-defined areas of the market.
- Having a history of over 300 index-tracking ETF holdings allows you to:
    1. build universes for stock-selection modeling (e.g. R3000, FTSE, etc.)
    2. proxy stock level exposures to MSCI GICS sectors and industries (via Sector ETFs)
    3. track market behavior (e.g. value vs growth)
    4. Anything else your curious quant heart desires :)
    
### Example Usage
```
from ishares.utils import s3_helpers, athena_helpers

# get a single file from s3
df_small = s3_helpers.get_etf_holdings('IWV', '2020-06-30')

# query a lot of data with Athena
df_big = athena_helpers.query('select * from qcdb.ishares_holdings '
                              'where etf=\'IWV\' '
                              'and asofdate between date \'2020-01-31\' and date \'2020-06-30\'')
```

### Project Details
#####  `config.py`
Contains critical AWS configuration parameters within the `Aws` class (e.g. `S3_ETF_HOLDINGS_BUCKET`, `AWS_KEY`, etc.)

#####  `ishares/build_etf_master_index.py`
This script scrapes the iShares landing page for their "universe" of ETFs. That source webpage (and resulting output) provides etf-level information (namely, inception dates and product page url's) required for downloading holding histories. Output is sent to `./data/ishares-etf-index.csv`
![./data/ishares-etf-index.csv](https://raw.githubusercontent.com/talsan/ishares/master/assets/img/ishares-etf-index.PNG)

#####  `ishares/queue_etfdownloaders.py`
This script builds a queue of events that are executed by `etfdownloader.py`. Specifically, for a given iShares ETF ticker, this script determines which holding dates need to be downloaded, based on which holdings were downloaded in prior sessions (with an `overwrite=True` parameter to re-process everything, if desired).

#####  `ishares/etfdownloader.py`
Given an ETF and a holdings date (i.e. a single "event"), this script downloads the csv, validates its structure, formats it, and uploads it to aws s3.

### S3 Storage Example

### Athena Query Output Example