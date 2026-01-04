# FlexiMart Data Architecture Project

**Student Name:** Pranav S  
**Student ID:** 25071971  
**Email:** (same as LMS submission)  
**Date:** 2026-01-04  

## Project Overview

This repository implements FlexiMart’s end-to-end data architecture assignment:

- **Part 1 (RDBMS + ETL):** Clean raw CSVs (customers/products/sales), load into MySQL using the provided schema, generate a data quality report, and answer required business questions using SQL.
- **Part 2 (NoSQL):** Justify MongoDB for a flexible product catalog and implement required MongoDB queries/updates on the provided JSON catalog.
- **Part 3 (Data Warehouse):** Design and implement a star schema in MySQL (`fleximart_dw`), load realistic dimensional + fact data, and run OLAP analytics queries.

All components are **reproducible** using Docker (MySQL + MongoDB) and scripts committed in this repo.


## Repository Structure


25071971-fleximart-data-architecture/
├── docker-compose.yml
├── .gitignore
├── README.md
│
├── data/
│   ├── customers_raw.csv
│   ├── products_raw.csv
│   ├── sales_raw.csv
│   └── products_catalog.json
│
├── part1-database-etl/
│   ├── etl_pipeline.py
│   ├── requirements.txt
│   ├── business_queries.sql
│   ├── data_quality_report.txt
│   └── schema_documentation.md
│
├── part2-nosql/
│   ├── README.md
│   ├── nosql_analysis.md
│   ├── mongodb_operations.js
│   └── products_catalog.json
│
└── part3-datawarehouse/
├── README.md
├── star_schema_design.md
├── warehouse_schema.sql
├── warehouse_data.sql
├── analytics_queries.sql
└── generate_warehouse_data.py


## Technologies Used

- **Python 3.x**
  - `pandas`
  - `mysql-connector-python`
  - `python-dateutil`
- **MySQL 8.0** (Docker container)
- **MongoDB 6.0 + mongosh** (Docker container)


## Prerequisites

Install these on your machine:

1. **Docker Desktop** (required)
2. **Python 3.x** (required)

No local installation of MySQL or MongoDB is required because both run in Docker.



## Step 1 — Start Databases (Docker)

From the repository root:

```bash
docker compose up -d
docker ps
````

Expected running containers:

* fleximart-mysql (MySQL on port 3306)
* fleximart-mongo (MongoDB on port 27017)


# Part 1 — Database Design + ETL Pipeline + Business Queries (MySQL)

## What Part 1 Does

### Data Sources

* data/customers_raw.csv
* data/products_raw.csv
* data/sales_raw.csv

### Target Database

* MySQL database: `fleximart`
* Tables created exactly as provided:

  * customers
  * products
  * orders
  * order_items

### Transform Rules Implemented (High-level)

* Remove duplicates (customers by `customer_id`, sales by `transaction_id`)
* Handle missing values:

  * Missing customer emails filled deterministically: `unknown+<customer_id>@fleximart.local`
  * Missing product prices imputed using category median (to satisfy NOT NULL)
  * Missing stock quantities set to `0`
* Standardize formats:

  * Phone standardized to `+91-XXXXXXXXXX`
  * Category standardized to `Electronics`, `Fashion`, `Groceries`
  * Dates parsed from mixed formats and stored as `YYYY-MM-DD`
* Sales → Orders mapping:

  * Each valid sales row becomes **one order + one order_item**
  * `subtotal = quantity * unit_price`, `total_amount = subtotal`
  * Sales rows with missing `customer_id` or missing `product_id` are dropped (cannot be linked)

### Output Artifact

* part1-database-etl/data_quality_report.txt (generated during ETL run)


## Part 1 — Setup Python Environment

From repo root:

```bash
python -m venv .venv
```

Activate:

**Windows PowerShell**

```powershell
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
pip install -r part1-database-etl/requirements.txt
```

## Part 1 — Run ETL

This script:

* creates database `fleximart` if needed
* creates tables if needed
* truncates tables for repeatable runs
* loads cleaned data
* generates the data quality report

```bash
python part1-database-etl/etl_pipeline.py
```


## Part 1 — Run Business Queries

These are the 3 required SQL queries stored in:

* `part1-database-etl/business_queries.sql`

**Windows PowerShell (used for this project)**

```powershell
Get-Content .\part1-database-etl\business_queries.sql -Raw | docker exec -i fleximart-mysql mysql -uroot -proot fleximart
```

Notes:

* MySQL may print a warning about using password on CLI; it is expected and not an error.
* Query output is printed to the terminal.


## Part 1 — Quick Sanity Checks (Optional)

Open MySQL shell:

```bash
docker exec -it fleximart-mysql mysql -uroot -proot
```

Run:

```sql
USE fleximart;

SHOW TABLES;

SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM products;
SELECT COUNT(*) FROM orders;
SELECT COUNT(*) FROM order_items;
```

# Part 2 — NoSQL Analysis + MongoDB Operations (MongoDB)

## What Part 2 Does

### Inputs

* part2-nosql/products_catalog.json (provided catalog with embedded reviews)

### Outputs

* Theory report: part2-nosql/nosql_analysis.md
* Practical script: part2-nosql/mongodb_operations.js

The MongoDB script performs:

1. Load JSON into `fleximart_nosql.products`
2. Query Electronics products with price < 50000 (projection: name, price, stock)
3. Aggregation: products with avg review rating >= 4.0
4. Update: push a new review into product `ELEC001`
5. Aggregation: average price by category (sorted desc)


## Part 2 — Run MongoDB Operations

Run:

```bash
docker exec -it fleximart-mongo mongosh fleximart_nosql /import/mongodb_operations.js
```

Notes:

* The script drops and reloads the `products` collection for clean re-runs.
* Output prints the results of each required operation.


# Part 3 — Data Warehouse + Analytics (MySQL Star Schema)

## What Part 3 Does

### Target Warehouse Database

* MySQL database: `fleximart_dw`

### Star Schema

* Dimensions:

  * dim_date (30 dates, Jan–Feb 2024)
  * dim_product (15 products across 3 categories)
  * dim_customer (12 customers across multiple cities/states)
* Fact:

  * fact_sales (40 transactions, with weekend lift and discounts)

### Files

* part3-datawarehouse/warehouse_schema.sql (schema creation)
* part3-datawarehouse/generate_warehouse_data.py (generates `warehouse_data.sql`, deterministic)
* part3-datawarehouse/warehouse_data.sql (insert statements)
* part3-datawarehouse/analytics_queries.sql (3 OLAP queries)


## Part 3 — Generate Warehouse Data (Deterministic)

This regenerates:

* part3-datawarehouse/warehouse_data.sql

```bash
python part3-datawarehouse/generate_warehouse_data.py
```


## Part 3 — Create Warehouse DB + Run Schema + Load Data + Run Analytics

**Windows PowerShell (used for this project):**

powershell
docker exec -i fleximart-mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS fleximart_dw; CREATE DATABASE fleximart_dw;"
Get-Content .\part3-datawarehouse\warehouse_schema.sql -Raw | docker exec -i fleximart-mysql mysql -uroot -proot fleximart_dw
Get-Content .\part3-datawarehouse\warehouse_data.sql -Raw   | docker exec -i fleximart-mysql mysql -uroot -proot fleximart_dw
Get-Content .\part3-datawarehouse\analytics_queries.sql -Raw| docker exec -i fleximart-mysql mysql -uroot -proot fleximart_dw

The OLAP query outputs are printed to the terminal:

1. Monthly drill-down (Year → Quarter → Month)
2. Top 10 products by revenue + revenue contribution %
3. Customer segmentation (High/Medium/Low value)


## Key Deliverables Checklist (All Included)

### Part 1

* part1-database-etl/etl_pipeline.py
* part1-database-etl/data_quality_report.txt
* part1-database-etl/business_queries.sql
* part1-database-etl/schema_documentation.md

### Part 2

* part2-nosql/nosql_analysis.md
* part2-nosql/mongodb_operations.js
* part2-nosql/products_catalog.json

### Part 3

* part3-datawarehouse/star_schema_design.md
* part3-datawarehouse/warehouse_schema.sql
* part3-datawarehouse/warehouse_data.sql
* part3-datawarehouse/analytics_queries.sql
* part3-datawarehouse/generate_warehouse_data.py

---

## Notes

* Databases are containerized for reproducibility. No local DB installation is required.
* PowerShell-friendly commands are used for running `.sql` files (to avoid `<` redirection issues on Windows).
* If the MySQL CLI prints warnings about password usage on CLI, it is expected and does not affect execution.

