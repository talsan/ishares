import os
from dotenv import load_dotenv

load_dotenv()


class Aws:
    # aws config
    S3_REGION_NAME = 'us-west-2'
    S3_ETF_HOLDINGS_BUCKET = 'ishares'
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
    ETF_MASTER_INDEX_LOC = './data/ishares-etf-index.csv'
    HOLDINGS_FILE_SCHEMAS = './data/ishares-file-schemas.json'

    # throttle requests to ishares.com
    MIN_SLEEP_BETWEEN_REQUESTS = 30
    MAX_SLEEP_BETWEEN_REQUESTS = 60


class AlphaVantage:
    AV_KEY = os.environ.get('AV_KEY')


# user agents that get randomly cycled through when making ishares.com download requests
USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
]
