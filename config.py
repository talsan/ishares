import os
from dotenv import load_dotenv

load_dotenv()


class Aws:
    S3_REGION_NAME = 'us-west-2'
    S3_ETF_HOLDINGS_BUCKET = 'ishares-etfs'
    S3_OBJECT_ROOT = 'https://s3.console.aws.amazon.com/s3/object'

    ATHENA_REGION_NAME = 'us-west-2'
    ATHENA_WORKGROUP = 'qc'
    ATHENA_OUTPUT_BUCKET = 'ishares-athena-output'

    ATHENA_SLEEP_BETWEEN_REQUESTS = 3
    ATHENA_QUERY_TIMEOUT = 200

    AWS_KEY = os.environ.get('AWS_KEY')
    AWS_SECRET = os.environ.get('AWS_SECRET')


class iShares:
    # fixed ishares.com values, the rest is derived/scraped
    ROOT = 'https://www.ishares.com'
    LANDING_PAGE = f'{ROOT}/us/products/etf-investments#!type=ishares&style=All&view=keyFacts'
    AJAX_REQUEST_CODE = '1467271812596.ajax'  # part of csv download request, subject to change (?)

    # first date that ishares.com makes holdings available
    FIRST_AVAILABLE_HOLDINGS_DATE = '2006-09-29'

    # output of build_etf_master_index.py is stored here
    # it contains all the ETFs (and their urls) in the ishares product line
    ETF_MASTER_INDEX_LOC = './ishares/data/ishares-etf-index.csv'

class ScrapingBee:
    SB_KEY = os.environ.get('SCRAPINGBEE_KEY')
    timeout = 30 # seconds
    max_retries = 5 # stop after 5 attempts (w/o a response within timeout window) to the same url
