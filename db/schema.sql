DROP TABLE IF EXISTS raw_sales;

CREATE TABLE raw_sales (
    "Row ID" INT,
    "Order ID" VARCHAR(50),
    "Order Date" DATE,
    "Ship Date" DATE,
    "Ship Mode" VARCHAR(50),
    "Customer ID" VARCHAR(50),
    "Customer Name" VARCHAR(100),
    "Segment" VARCHAR(50),
    "Country" VARCHAR(50),
    "City" VARCHAR(100),
    "State" VARCHAR(50),
    "Postal Code" INT,
    "Region" VARCHAR(50),
    "Product ID" VARCHAR(50),
    "Category" VARCHAR(50),
    "Sub-Category" VARCHAR(50),
    "Product Name" VARCHAR(200),
    "Sales" NUMERIC
);
