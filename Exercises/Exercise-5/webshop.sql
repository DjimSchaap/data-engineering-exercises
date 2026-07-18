DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS products;

CREATE TABLE accounts (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    address_1 VARCHAR(255) NOT NULL,
    address_2 VARCHAR,
    city VARCHAR(255) NOT NULL,
    state VARCHAR(255) NOT NULL,
    zip_code VARCHAR(20) NOT NULL,
    join_date DATE NOT NULL
);

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_code VARCHAR(255) NOT NULL UNIQUE,
    product_description VARCHAR(255)
);

CREATE TABLE transactions (
    transaction_id VARCHAR(255) PRIMARY KEY,
    transaction_date DATE NOT NULL,
    quantity INT,
    product_id INT,
    account_id INT,
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(product_id),
    CONSTRAINT fk_account FOREIGN KEY (account_id) REFERENCES accounts(customer_id)
);

CREATE INDEX accounts_last_name_index ON accounts (last_name);
CREATE INDEX products_product_description_index ON products (product_description);
CREATE INDEX transactions_transaction_date_index ON transactions (transaction_date);
CREATE INDEX transactions_product_id_index ON transactions (product_id);
CREATE INDEX transactions_account_id_index ON transactions (account_id);
