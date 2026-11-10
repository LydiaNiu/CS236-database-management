CREATE TABLE NYC ();


CREATE TABLE LA ();

CREATE TABLE unified_NYC_LA ();

CREATE TABLE users (
    login VARCHAR(50) PRIMARY KEY,
    password VARCHAR(100) NOT NULL,
    phone_num VARCHAR(20) NOT NULL,
    address VARCHAR(255) NOT NULL,
    role VARCHAR(10) NOT NULL DEFAULT 'Buyer'
        CHECK (role IN ('Buyer', 'Seller', 'Admin')),
    favorite_category VARCHAR(50),
    UNIQUE (login, role)
);