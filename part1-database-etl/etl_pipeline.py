#!/usr/bin/env python3
"""
FlexiMart ETL Pipeline (Part 1)

Reads raw CSV files:
- data/customers_raw.csv
- data/products_raw.csv
- data/sales_raw.csv

Cleans data quality issues and loads into MySQL database "fleximart"
using the exact schema provided in the assignment.

Outputs:
- part1-database-etl/data_quality_report.txt
"""

import os
import re
import logging
from decimal import Decimal, ROUND_HALF_UP
from dataclasses import dataclass
from pathlib import Path
from datetime import date

import pandas as pd
import mysql.connector


# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("fleximart-etl")


# ---------------- Config ----------------
REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
OUT_REPORT = Path(__file__).resolve().parent / "data_quality_report.txt"

CUSTOMERS_CSV = DATA_DIR / "customers_raw.csv"
PRODUCTS_CSV  = DATA_DIR / "products_raw.csv"
SALES_CSV     = DATA_DIR / "sales_raw.csv"


@dataclass
class DBConfig:
    host: str = os.getenv("FLEXIMART_DB_HOST", "127.0.0.1")
    port: int = int(os.getenv("FLEXIMART_DB_PORT", "3306"))
    user: str = os.getenv("FLEXIMART_DB_USER", "root")
    password: str = os.getenv("FLEXIMART_DB_PASSWORD", "root")
    database: str = os.getenv("FLEXIMART_DB_NAME", "fleximart")


# ---------------- Helpers ----------------
def normalize_spaces(s: str) -> str:
    if s is None or pd.isna(s):
        return ""
    s = str(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_city(s: str) -> str:
    s = normalize_spaces(s)
    return s.title() if s else ""

def normalize_category(s: str) -> str:
    s = normalize_spaces(s).lower()
    mapping = {
        "electronics": "Electronics",
        "fashion": "Fashion",
        "groceries": "Groceries"
    }
    return mapping.get(s, s.title() if s else "")

def normalize_phone(phone: str) -> str | None:
    """
    Standardize to +91-XXXXXXXXXX (10 digits).
    Handles:
    - 9876543210
    - +91-9988776655
    - 09988112233
    - +919876501234
    - 09871234567
    """
    if phone is None or pd.isna(phone) or str(phone).strip() == "":
        return None
    digits = re.sub(r"\D+", "", str(phone))
    # remove leading country code or 0-prefix
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[-10:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[-10:]
    elif len(digits) > 10:
        digits = digits[-10:]
    if len(digits) != 10:
        return None
    return f"+91-{digits}"

def parse_flex_date(s: str) -> date | None:
    """
    Parse inconsistent date formats safely:
    - YYYY-MM-DD
    - DD/MM/YYYY
    - MM-DD-YYYY
    - MM/DD/YYYY
    Uses rule-based logic to handle ambiguity.
    """
    if s is None or pd.isna(s) or str(s).strip() == "":
        return None
    s = normalize_spaces(s)

    # Fast path: YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        try:
            return pd.to_datetime(s, format="%Y-%m-%d").date()
        except Exception:
            return None

    # Identify delimiter
    delim = "/" if "/" in s else ("-" if "-" in s else None)
    if not delim:
        # last resort
        try:
            return pd.to_datetime(s).date()
        except Exception:
            return None

    parts = s.split(delim)
    if len(parts) != 3:
        try:
            return pd.to_datetime(s).date()
        except Exception:
            return None

    a, b, c = parts[0], parts[1], parts[2]

    # If starts with year
    if len(a) == 4:
        try:
            return pd.to_datetime(s, format="%Y-%m-%d").date()
        except Exception:
            try:
                return pd.to_datetime(s).date()
            except Exception:
                return None

    # Otherwise likely DD/MM/YYYY or MM-DD-YYYY / MM/DD/YYYY
    try:
        ai = int(a)
        bi = int(b)
    except Exception:
        try:
            return pd.to_datetime(s).date()
        except Exception:
            return None

    # Rule:
    # - if second part > 12 => it's month-first (MM-DD-YYYY or MM/DD/YYYY)
    # - if first part > 12 => it's day-first (DD/MM/YYYY or DD-MM-YYYY)
    # - else ambiguous => choose month-first (fits this dataset best)
    dayfirst = False
    if bi > 12:
        dayfirst = False
    elif ai > 12:
        dayfirst = True
    else:
        dayfirst = False

    try:
        return pd.to_datetime(s, dayfirst=dayfirst).date()
    except Exception:
        return None

def to_decimal_2(x) -> Decimal:
    d = Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return d


# ---------------- DB Setup ----------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    city VARCHAR(50),
    registration_date DATE
);

CREATE TABLE IF NOT EXISTS products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock_quantity INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'Pending',
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    subtotal DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
"""


def get_conn(cfg: DBConfig, use_db: bool) -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=(cfg.database if use_db else None),
        autocommit=False
    )


def ensure_database_and_schema(cfg: DBConfig):
    # Create DB
    conn0 = get_conn(cfg, use_db=False)
    cur0 = conn0.cursor()
    cur0.execute(f"CREATE DATABASE IF NOT EXISTS {cfg.database};")
    conn0.commit()
    cur0.close()
    conn0.close()

    # Create tables
    conn = get_conn(cfg, use_db=True)
    cur = conn.cursor()
    for stmt in SCHEMA_SQL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)
    conn.commit()
    cur.close()
    conn.close()


def truncate_tables(conn):
    cur = conn.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS=0;")
    cur.execute("TRUNCATE TABLE order_items;")
    cur.execute("TRUNCATE TABLE orders;")
    cur.execute("TRUNCATE TABLE products;")
    cur.execute("TRUNCATE TABLE customers;")
    cur.execute("SET FOREIGN_KEY_CHECKS=1;")
    conn.commit()
    cur.close()


# ---------------- ETL ----------------
def run_etl():
    # ---- Extract ----
    for p in [CUSTOMERS_CSV, PRODUCTS_CSV, SALES_CSV]:
        if not p.exists():
            raise FileNotFoundError(f"Missing file: {p}. Put raw files in repo /data/ folder.")

    customers = pd.read_csv(CUSTOMERS_CSV, dtype=str)
    products  = pd.read_csv(PRODUCTS_CSV, dtype=str)
    sales     = pd.read_csv(SALES_CSV, dtype=str)

    # Strip spaces for all string columns
    customers = customers.applymap(normalize_spaces)
    products  = products.applymap(normalize_spaces)
    sales     = sales.applymap(normalize_spaces)

    report = {
        "customers_raw": len(customers),
        "products_raw": len(products),
        "sales_raw": len(sales),

        "customers_dupes_removed": 0,
        "products_dupes_removed": 0,
        "sales_dupes_removed": 0,

        "customers_missing_email_filled": 0,
        "products_missing_price_imputed": 0,
        "products_missing_stock_filled": 0,

        "sales_missing_customer_dropped": 0,
        "sales_missing_product_dropped": 0,
        "sales_invalid_date_dropped": 0,

        "customers_loaded": 0,
        "products_loaded": 0,
        "orders_loaded": 0,
        "order_items_loaded": 0,
    }

    # ---- Transform: Customers ----
    before = len(customers)
    customers = customers.drop_duplicates(subset=["customer_id"], keep="first")
    report["customers_dupes_removed"] = before - len(customers)

    # Fill missing emails with deterministic placeholders (keeps referential integrity with sales)
    missing_email_mask = (customers["email"].isna()) | (customers["email"] == "")
    report["customers_missing_email_filled"] = int(missing_email_mask.sum())
    customers.loc[missing_email_mask, "email"] = customers.loc[missing_email_mask, "customer_id"].apply(
        lambda cid: f"unknown+{cid.lower()}@fleximart.local"
    )

    customers["email"] = customers["email"].str.lower()
    customers["phone"] = customers["phone"].apply(normalize_phone)
    customers["city"] = customers["city"].apply(normalize_city)
    customers["registration_date"] = customers["registration_date"].apply(parse_flex_date)

    # Ensure required fields present
    customers["first_name"] = customers["first_name"].apply(normalize_spaces)
    customers["last_name"]  = customers["last_name"].apply(normalize_spaces)

    # ---- Transform: Products ----
    before = len(products)
    products = products.drop_duplicates(subset=["product_id"], keep="first")
    report["products_dupes_removed"] = before - len(products)

    products["product_name"] = products["product_name"].apply(normalize_spaces)
    products["category"] = products["category"].apply(normalize_category)

    # Price -> numeric, impute missing by category median
    products["price_num"] = pd.to_numeric(products["price"], errors="coerce")
    missing_price_mask = products["price_num"].isna()
    report["products_missing_price_imputed"] = int(missing_price_mask.sum())

    medians = products.groupby("category")["price_num"].median(numeric_only=True)
    def impute_price(row):
        if pd.isna(row["price_num"]):
            m = medians.get(row["category"], None)
            return m if pd.notna(m) else None
        return row["price_num"]

    products["price_num"] = products.apply(impute_price, axis=1)

    # If still missing (category had all missing), drop (should not happen in this dataset)
    products = products.dropna(subset=["price_num"])

    # Stock -> int, fill missing with 0
    products["stock_num"] = pd.to_numeric(products["stock_quantity"], errors="coerce")
    missing_stock_mask = products["stock_num"].isna()
    report["products_missing_stock_filled"] = int(missing_stock_mask.sum())
    products["stock_num"] = products["stock_num"].fillna(0).astype(int)

    # ---- Transform: Sales ----
    before = len(sales)
    sales = sales.drop_duplicates(subset=["transaction_id"], keep="first")
    report["sales_dupes_removed"] = before - len(sales)

    missing_cust = (sales["customer_id"].isna()) | (sales["customer_id"] == "")
    report["sales_missing_customer_dropped"] = int(missing_cust.sum())
    sales = sales[~missing_cust].copy()

    missing_prod = (sales["product_id"].isna()) | (sales["product_id"] == "")
    report["sales_missing_product_dropped"] = int(missing_prod.sum())
    sales = sales[~missing_prod].copy()

    sales["quantity_num"] = pd.to_numeric(sales["quantity"], errors="coerce").fillna(0).astype(int)
    sales = sales[sales["quantity_num"] > 0].copy()

    sales["unit_price_num"] = pd.to_numeric(sales["unit_price"], errors="coerce")
    sales = sales.dropna(subset=["unit_price_num"]).copy()

    sales["transaction_date_parsed"] = sales["transaction_date"].apply(parse_flex_date)
    invalid_dates = sales["transaction_date_parsed"].isna()
    report["sales_invalid_date_dropped"] = int(invalid_dates.sum())
    sales = sales[~invalid_dates].copy()

    # Keep only sales that refer to known raw IDs (after dedup)
    valid_customer_ids = set(customers["customer_id"].tolist())
    valid_product_ids  = set(products["product_id"].tolist())

    sales = sales[sales["customer_id"].isin(valid_customer_ids)].copy()
    sales = sales[sales["product_id"].isin(valid_product_ids)].copy()

    # ---- Load ----
    cfg = DBConfig()
    log.info("Ensuring database + schema exist...")
    ensure_database_and_schema(cfg)

    conn = get_conn(cfg, use_db=True)
    try:
        truncate_tables(conn)

        cur = conn.cursor()

        # Insert customers and build mapping raw_customer_id -> surrogate customer_id (int)
        cust_map = {}
        cust_sql = """
        INSERT INTO customers (first_name, last_name, email, phone, city, registration_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        for _, r in customers.iterrows():
            cur.execute(cust_sql, (
                r["first_name"],
                r["last_name"],
                r["email"],
                r["phone"],
                r["city"],
                r["registration_date"]
            ))
            cust_map[r["customer_id"]] = cur.lastrowid

        report["customers_loaded"] = len(cust_map)

        # Insert products and build mapping raw_product_id -> surrogate product_id (int)
        prod_map = {}
        prod_sql = """
        INSERT INTO products (product_name, category, price, stock_quantity)
        VALUES (%s, %s, %s, %s)
        """
        for _, r in products.iterrows():
            cur.execute(prod_sql, (
                r["product_name"],
                r["category"],
                str(to_decimal_2(r["price_num"])),
                int(r["stock_num"])
            ))
            prod_map[r["product_id"]] = cur.lastrowid

        report["products_loaded"] = len(prod_map)

        # Insert orders + order_items
        order_sql = """
        INSERT INTO orders (customer_id, order_date, total_amount, status)
        VALUES (%s, %s, %s, %s)
        """
        item_sql = """
        INSERT INTO order_items (order_id, product_id, quantity, unit_price, subtotal)
        VALUES (%s, %s, %s, %s, %s)
        """

        orders_loaded = 0
        items_loaded = 0

        for _, r in sales.iterrows():
            cust_pk = cust_map.get(r["customer_id"])
            prod_pk = prod_map.get(r["product_id"])
            if not cust_pk or not prod_pk:
                continue

            qty = int(r["quantity_num"])
            unit_price = to_decimal_2(r["unit_price_num"])
            subtotal = to_decimal_2(unit_price * Decimal(qty))

            cur.execute(order_sql, (
                cust_pk,
                r["transaction_date_parsed"],
                str(subtotal),
                normalize_spaces(r.get("status", "Pending")).title() or "Pending"
            ))
            order_id = cur.lastrowid
            orders_loaded += 1

            cur.execute(item_sql, (
                order_id,
                prod_pk,
                qty,
                str(unit_price),
                str(subtotal)
            ))
            items_loaded += 1

        report["orders_loaded"] = orders_loaded
        report["order_items_loaded"] = items_loaded

        conn.commit()
        cur.close()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    # ---- Write Data Quality Report ----
    OUT_REPORT.write_text(
        "\n".join([
            "FlexiMart Data Quality Report (Generated by ETL)\n",
            f"Customers raw records: {report['customers_raw']}",
            f"Customers duplicates removed: {report['customers_dupes_removed']}",
            f"Customers missing emails filled: {report['customers_missing_email_filled']}",
            f"Customers loaded: {report['customers_loaded']}",
            "",
            f"Products raw records: {report['products_raw']}",
            f"Products duplicates removed: {report['products_dupes_removed']}",
            f"Products missing prices imputed: {report['products_missing_price_imputed']}",
            f"Products missing stock filled with 0: {report['products_missing_stock_filled']}",
            f"Products loaded: {report['products_loaded']}",
            "",
            f"Sales raw records: {report['sales_raw']}",
            f"Sales duplicates removed: {report['sales_dupes_removed']}",
            f"Sales missing customer_id dropped: {report['sales_missing_customer_dropped']}",
            f"Sales missing product_id dropped: {report['sales_missing_product_dropped']}",
            f"Sales invalid date dropped: {report['sales_invalid_date_dropped']}",
            f"Orders loaded: {report['orders_loaded']}",
            f"Order items loaded: {report['order_items_loaded']}",
            "",
            "Notes:",
            "- Missing customer emails were filled with deterministic placeholders: unknown+<customer_id>@fleximart.local",
            "- Each sales transaction row becomes one order and one order_item (line-item grain).",
        ]),
        encoding="utf-8"
    )

    log.info("✅ ETL complete.")
    log.info(f"✅ Data quality report written to: {OUT_REPORT}")


if __name__ == "__main__":
    run_etl()
