CREATE TABLE IF NOT EXISTS  purchase_orders(
order_id INT PRIMARY KEY NOT NULL,
order_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS line_items(
id SERIAL PRIMARY KEY NOT NULL,
order_id INT,
product VARCHAR,
product_ean VARCHAR,
quantity INT,
price REAL,
cost_price REAL,
price_total REAL,
CONSTRAINT fk_purchase_orders FOREIGN KEY(order_id) REFERENCES purchase_orders(order_id)
);

CREATE TABLE IF NOT EXISTS order_address(
id SERIAL PRIMARY KEY NOT NULL,
order_id INT,
street TEXT,
city VARCHAR,
state VARCHAR,
zip VARCHAR,
CONSTRAINT fk_purchase_orders FOREIGN KEY(order_id) REFERENCES purchase_orders(order_id)
);

