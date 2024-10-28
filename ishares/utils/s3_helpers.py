import boto3
from botocore.exceptions import ClientError
from config import Aws
import pandas as pd
from io import BytesIO
import re
import logging
log = logging.getLogger(__name__)

# GLOBAL DATA (AWS CLIENT)
aws_session = boto3.Session(aws_access_key_id=Aws.AWS_KEY,
                            aws_secret_access_key=Aws.AWS_SECRET)

s3_client = aws_session.client('s3', region_name=Aws.S3_REGION_NAME)

def list_keys(Bucket, Prefix='', Suffix='', full_path=True, remove_ext=False):
    # get pages for bucket and prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=Bucket, Prefix=Prefix)

    # iterate through pages and store the keys in a list
    keys = []
    for page in page_iterator:
        if 'Contents' in page.keys():
            for content in page['Contents']:
                key = content['Key']
                if not key.endswith('/'):  # ignore directories
                    if key.endswith(Suffix):
                        if not full_path:
                            key = re.sub(Prefix, '', key)
                        if remove_ext:
                            key = re.sub('\.[^.]+$','',key)
                        keys.append(key)
    return keys

def get_etf_holdings(etf_ticker,asofdate):

    s3_key = f'type=holdings/state=formatted/etf={etf_ticker}/asofdate={asofdate}.csv'

    try:
        obj = s3_client.get_object(Bucket=Aws.S3_ETF_HOLDINGS_BUCKET,
                                   Key=s3_key)
    except ClientError as e:
        log.error(f'{e.response["Error"]["Code"]}\n'
                  f'cant find file {s3_key}')
        raise
    df = pd.read_csv(BytesIO(obj['Body'].read()), dtype=str)
    return df
