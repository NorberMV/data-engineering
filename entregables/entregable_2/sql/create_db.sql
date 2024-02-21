DROP TABLE IF EXISTS {full_schema};
CREATE TABLE {full_schema} (
    Date TIMESTAMP,
    prices FLOAT,
    market_caps FLOAT,
    total_volumes FLOAT
);