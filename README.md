# Scrape, Store, and Query iShares ETF holding files
This document outlines the iShares ETF data pipeline and the code that powers it. If you want to understand the context behind this project, see [`READMEQuant.md`](https://github.com/talsan/ishares/blob/master/READMEQuant.md)
## Process Overview
1. Data Pipeline: Download a full monthly history of ETF holdings from iShares.com, for every ETF in the iShares product line. [Full list of ETFs here.](https://github.com/talsan/ishares/blob/master/ishares/data/ishares-etf-index.csv). History available from 2006 to present.
2. Data Access: Persist data in S3 for direct access, and/or Athena for querying facilities. [Examples here.](https://github.com/talsan/ishares#example-usage)

## Features & Options
#### Event-driven design:
- [`etf_downloader.py`](https://github.com/talsan/ishares#etf_downloaderpy) - a self-contained event for a given ticker & date
- [`batch_etf_downloader.py`](https://github.com/talsan/ishares#batch_etf_downloaderpy)  - a batch process that queues-and-invokes a series of etf-downloaders (i.e. ticker-date combinations). The batch process can be run at any time and any frequency to keep etf holdings up to date.
#### AWS Integration: 
- Option to run process using local directories or S3 (via `outputpath` parameter)
- If S3, an Athena ddl (`athenadll.sql`) is available for Table Creation, and simple boto3 wrappers (`ishares.utils.athena_helpers`) for SQL Querying.
#### Gentle Scraping
- Randomized sleep time after requests (lower/upper bound is configurable in `config.py`)
- Randomized user-agents in request headers (configurable in `config.py`)

## Data Pipeline Details
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
