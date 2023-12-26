CREATE TABLE IF NOT EXISTS customers(
    customerid INT PRIMARY KEY,
    name VARCHAR(50),
    occupation VARCHAR(50),
    email VARCHAR(50),
    company VARCHAR(50),
    phonenumber VARCHAR(20),
    age INT
);

CREATE TABLE IF NOT EXISTS agents(
    agentid INT PRIMARY KEY,
    name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS calls(
    callid INT PRIMARY KEY,
    agentid INT,
    customerid INT,
    pickedup SMALLINT,
    duration INT,
    productsold SMALLINT
);

COPY customers FROM '/opt/csv/customers.csv' DELIMITER ',' CSV HEADER;
COPY agents FROM '/opt/csv/agents.csv' DELIMITER ',' CSV HEADER;
COPY calls FROM '/opt/csv/calls.csv' DELIMITER ',' CSV HEADER;