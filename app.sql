CREATE DATABASE IF NOT EXISTS product_db;

USE product_db;

CREATE TABLE IF NOT EXISTS products (
    ProductID INT PRIMARY KEY,
    Name VARCHAR(100),
    Category VARCHAR(100),
    Rating FLOAT,
    Reviews INT,
    Brand VARCHAR(50),
    Stock INT,
    LaunchDate DATE,
    Discount FLOAT,
    Price FLOAT
);
