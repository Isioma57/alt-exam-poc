
-- Create schema
CREATE SCHEMA IF NOT EXISTS ALT_SCHOOL;



-- create and populate tables
create table if not exists ALT_SCHOOL.PRODUCTS
(
    id  serial primary key,
    name varchar not null,
    price numeric(10, 2) not null
);

COPY ALT_SCHOOL.PRODUCTS (id, name, price)
FROM '/data/products.csv' DELIMITER ',' CSV HEADER;




-- TODO: Provide the DDL statment to create this table ALT_SCHOOL.CUSTOMERS

CREATE TABLE IF NOT EXISTS ALT_SCHOOL.CUSTOMERS
(
    customer_id uuid PRIMARY KEY,
    device_id uuid,
    location varchar,
    currency varchar           
);
-- Copy the data to populate the table
COPY ALT_SCHOOL.CUSTOMERS (customer_id, device_id, location, currency)
FROM '/data/customers.csv' DELIMITER ',' CSV HEADER;




-- TODO: complete the table DDL statement

create table if not exists ALT_SCHOOL.ORDERS
(
    order_id uuid not null primary key,
    customer_id uuid,
    status varchar,
    checked_out_at timestamp
);
-- Copy the data to populate the table
COPY ALT_SCHOOL.ORDERS (order_id, customer_id, status, checked_out_at)
FROM '/data/orders.csv' DELIMITER ',' CSV HEADER;




-- TODO: complete the table DDL statement
create table if not exists ALT_SCHOOL.LINE_ITEMS
(
    line_item_id serial primary key,
    order_id uuid,
    item_id bigint,
    quantity bigint
);
-- Copy the data to populate the table
COPY ALT_SCHOOL.LINE_ITEMS (line_item_id, order_id, item_id, quantity)
FROM '/data/line_items.csv' DELIMITER ',' CSV HEADER;




-- setup the events table following the example provided
create table if not exists ALT_SCHOOL.EVENTS
(
    event_id bigint PRIMARY KEY,
    customer_id uuid,
    event_data jsonb,
    event_timestamp timestamp
);
-- Copy the data to populate the table
COPY ALT_SCHOOL.EVENTS (event_id, customer_id, event_data, event_timestamp)
FROM '/data/events.csv' DELIMITER ',' CSV HEADER;















