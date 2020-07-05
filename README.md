### A Simple iShares ETF Holdings Data Pipeline
by Tal Sansani & Vinod Chandrashakeran
#### What does this do?
- Collect a full monthly history (back to 2006) of ETF holdings from iShares.com, for every ETF in the iShares product line
- Persists data in S3 in such a way that enables codified direct access and Athena querying facilities
#### Why would you ever do this?
- iShares ETF holdings track well-defined indices - baskets of stocks that represent well-defined areas of the market.
- Having a history of over 300 index-tracking ETF holdings allows you to:
    1. build universes for stock-selection modeling (eg. R3000, FTSE, etc.)
    2. proxy stock level exposures to MSCI GICS sectors and industries (via Sector ETFs)
    3. track market behavior (eg. value vs growth)
    4. Anything else your curious quant heart desires :)
#### What's it look like?
```
from ishares.utils import s3_helpers, athena_helpers

# get a single file from s3
df_small = s3_helpers.get_etf_holdings('IWV', '2020-06-30')

# query a lot of data with Athena
df_big = athena_helpers.query('select * from qcdb.ishares_holdings '
                              'where etf=\'IWV\' '
                              'and asofdate between date \'2020-01-31\' and date \'2020-06-30\'')
```

#### Key File Explainers
######  `config.py`
Contains critical AWS configuration parameters within the `Aws` class (eg. `S3_ETF_HOLDINGS_BUCKET`, `AWS_KEY`, etc.)

######  `ishares/build_etf_master_index.py`
This script scrapes the iShares landing page for their "universe" of ETFs. That source webpage (and resulting output) provides etf-level information (namely, inception dates and product page url's) required for downloading holding histories. Output is sent to `./data/ishares-etf-index.csv`
![./data/ishares-etf-index.csv](https://raw.githubusercontent.com/talsan/ishares/master/assets/img/ishares-etf-index.PNG)

######  `ishares/queue_etfdownloaders.py`
This script builds a queue of events, that are executed by `etfdownloader.py`. Specifically, for a given iShares ETF ticker, this script determines which holding dates need to be downloaded, based on which holdings were downloaded in prior sessions (with an `overwrite=True` parameter to re-process everything, if desired).

######  `ishares/etfdownloader.py`
Given an ETF and a holdings date, this script downloads the csv, validates its structure, formats it, and uploads it to aws s3.