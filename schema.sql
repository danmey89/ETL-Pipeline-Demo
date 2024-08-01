CREATE TABLE IF NOT EXISTS  purchase_orders(
sequence SERIAL PRIMARY KEY,
order_id INT,
order_date TIMESTAMP,
product VARCHAR,
product_ean VARCHAR,
quantity INT,
street TEXT,
city VARCHAR,
state VARCHAR,
zip VARCHAR,
price REAL,
cost_price REAL,
price_total REAL
);