CREATE EXTERNAL TABLE IF NOT EXISTS glassdoor.ishares_etf_holdings (
  `ticker` string,
  `etf` string,
  `holdings_date` date,
  `name` string,
  `sector` string,
  `asset_class` string,
  `market_value` float,
  `weight` float,
  `notional_value` float,
  `shares` float,
  `cusip` string,
  `isin` string,
  `sedol` string,
  `price` float,
  `location` string,
  `exchange` string,
  `currency` string,
  `fx_rate` float,
  `maturity` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://ishares-etfs/type=holdings/state=formatted'
TBLPROPERTIES ('has_encrypted_data'='false',
               'skip.header.line.count'='1');