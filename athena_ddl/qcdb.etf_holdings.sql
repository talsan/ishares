-- table definition file (ddl)
-- no python code depends on it, but if we ever need to re-create or edit the etf_holdings table, this is where
-- we'd start

CREATE EXTERNAL TABLE IF NOT EXISTS qcdb.etf_holdings (
  `asofdate` date,
  `ticker` string,
  `name` string,
  `asset_class` string,
  `weight` float,
  `price` float,
  `shares` float,
  `market_value` float,
  `notional_value` float,
  `sector` string,
  `sedol` string,
  `isin` string,
  `exchange` string
) PARTITIONED BY (
  source string,
  etf string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://etf-holdings/'
TBLPROPERTIES ('has_encrypted_data'='false',
               'skip.header.line.count'='1');

-- MSCK REPAIR TABLE qcdb.etf_holdings;