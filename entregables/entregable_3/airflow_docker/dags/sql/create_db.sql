DROP TABLE IF EXISTS norbermv_dev_coderhouse.bitcoin_data;
CREATE TABLE norbermv_dev_coderhouse.bitcoin_data (
    Date TIMESTAMP,
    prices FLOAT,
    market_caps FLOAT,
    total_volumes FLOAT
);