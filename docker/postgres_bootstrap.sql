CREATE SCHEMA source;

-- default directory for every statement in this bootstrap
SET search_path TO source;

-- Create the sales data tables
CREATE TABLE customers (
    customer_id serial PRIMARY KEY,
    customer_name VARCHAR(100),
    customer_email VARCHAR(100),
    created_at timestamp default current_timestamp
);

-- Insert some dummy data
INSERT INTO customers (customer_name, customer_email)
VALUES
    ('Johnny Allstar', 'customer1@example.com'),
    ('Aubrey Plaza', 'customer2@example.com'),
    ('John Wick', 'customer3@example.com');
