import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_SQL = SCRIPT_DIR / "warehouse_data.sql"

def sql_escape(s: str) -> str:
    """Escape single quotes for MySQL string literals."""
    return str(s).replace("'", "''")

def sql_str(s: str) -> str:
    """Wrap escaped string in single quotes."""
    return f"'{sql_escape(s)}'"

def date_key(d: date) -> int:
    return int(d.strftime("%Y%m%d"))

def quarter(m: int) -> str:
    return f"Q{((m - 1) // 3) + 1}"

# ---- dim_date: 30 dates spanning Jan-Feb 2024 ----
start = date(2024, 1, 15)  # spans both Jan + Feb
dates = [start + timedelta(days=i) for i in range(30)]

dim_date_rows = []
for d in dates:
    dim_date_rows.append({
        "date_key": date_key(d),
        "full_date": d.isoformat(),
        "day_of_week": d.strftime("%A"),
        "day_of_month": d.day,
        "month": d.month,
        "month_name": d.strftime("%B"),
        "quarter": quarter(d.month),
        "year": d.year,
        "is_weekend": 1 if d.weekday() >= 5 else 0
    })

# ---- dim_product: 15 products across 3 categories (prices within 100..100000) ----
products = [
    ("P001", "Samsung Galaxy S21",       "Electronics", "Smartphones", 45999.00),
    ("P003", "Apple MacBook Pro",        "Electronics", "Laptops",     99999.00),
    ("P005", "Sony Headphones",          "Electronics", "Audio",        1999.00),
    ("P007", "HP Laptop",                "Electronics", "Laptops",     52999.00),
    ("P012", "Dell Monitor 24inch",      "Electronics", "Monitors",    12999.00),
    ("P014", "iPhone 13",                "Electronics", "Smartphones", 69999.00),

    ("P002", "Nike Running Shoes",       "Fashion",     "Footwear",     3499.00),
    ("P004", "Levi's Jeans",             "Fashion",     "Clothing",     2999.00),
    ("P008", "Adidas T-Shirt",           "Fashion",     "Clothing",     1299.00),
    ("P011", "Puma Sneakers",            "Fashion",     "Footwear",     4599.00),
    ("P013", "Woodland Shoes",           "Fashion",     "Footwear",     5499.00),

    ("P006", "Organic Almonds",          "Groceries",   "Dry Fruits",    899.00),
    ("P009", "Basmati Rice 5kg",         "Groceries",   "Staples",       650.00),
    ("P015", "Organic Honey 500g",       "Groceries",   "Condiments",    450.00),
    ("P018", "Masoor Dal 1kg",           "Groceries",   "Staples",       120.00),
]

# ---- dim_customer: 12 customers across multiple cities/states ----
customers = [
    ("C001", "Rahul Sharma",   "Bangalore",  "Karnataka",    "Retail"),
    ("C002", "Priya Patel",    "Mumbai",     "Maharashtra",  "Premium"),
    ("C003", "Amit Kumar",     "Delhi",      "Delhi",        "Retail"),
    ("C004", "Sneha Reddy",    "Hyderabad",  "Telangana",    "Premium"),
    ("C005", "Vikram Singh",   "Chennai",    "Tamil Nadu",   "Retail"),
    ("C006", "Anjali Mehta",   "Bangalore",  "Karnataka",    "Premium"),
    ("C009", "Karthik Nair",   "Kochi",      "Kerala",       "Retail"),
    ("C011", "Arjun Rao",      "Hyderabad",  "Telangana",    "Premium"),
    ("C013", "Suresh Patel",   "Mumbai",     "Maharashtra",  "Retail"),
    ("C014", "Neha Shah",      "Ahmedabad",  "Gujarat",      "Retail"),
    ("C017", "Rajesh Kumar",   "Delhi",      "Delhi",        "Premium"),
    ("C020", "Swati Desai",    "Pune",       "Maharashtra",  "Retail"),
]

# Keys will be 1..N because of AUTO_INCREMENT, assuming dims are truncated before inserts
product_keys = list(range(1, len(products) + 1))
customer_keys = list(range(1, len(customers) + 1))

# ---- fact_sales: 40 rows with weekend lift + discounts ----
weighted_date_keys = []
for r in dim_date_rows:
    w = 3 if r["is_weekend"] == 1 else 1
    weighted_date_keys.extend([r["date_key"]] * w)

facts = []
for _ in range(40):
    dk = random.choice(weighted_date_keys)
    pk = random.choice(product_keys)
    ck = random.choice(customer_keys)

    unit_price = float(products[pk - 1][4])
    qty = random.choice([1, 1, 1, 2, 2, 3, 4])

    is_weekend = next(r["is_weekend"] for r in dim_date_rows if r["date_key"] == dk)
    disc_rate = random.choice([0, 0, 0.05, 0.10]) if is_weekend else random.choice([0, 0, 0.05])

    discount = round(qty * unit_price * disc_rate, 2)
    total = round(qty * unit_price - discount, 2)

    facts.append((dk, pk, ck, qty, unit_price, discount, total))

# ---- Write SQL file ----
out = []
out.append("USE fleximart_dw;")
out.append("SET FOREIGN_KEY_CHECKS=0;")
out.append("TRUNCATE TABLE fact_sales;")
out.append("TRUNCATE TABLE dim_customer;")
out.append("TRUNCATE TABLE dim_product;")
out.append("TRUNCATE TABLE dim_date;")
out.append("SET FOREIGN_KEY_CHECKS=1;")
out.append("")

out.append("-- dim_date (30 rows: Jan-Feb 2024)")
for r in dim_date_rows:
    out.append(
        "INSERT INTO dim_date (date_key, full_date, day_of_week, day_of_month, month, month_name, quarter, year, is_weekend) "
        f"VALUES ({r['date_key']}, {sql_str(r['full_date'])}, {sql_str(r['day_of_week'])}, {r['day_of_month']}, {r['month']}, "
        f"{sql_str(r['month_name'])}, {sql_str(r['quarter'])}, {r['year']}, {r['is_weekend']});"
    )

out.append("")
out.append("-- dim_product (15 rows across 3 categories)")
for pid, pname, cat, sub, price in products:
    out.append(
        "INSERT INTO dim_product (product_id, product_name, category, subcategory, unit_price) "
        f"VALUES ({sql_str(pid)}, {sql_str(pname)}, {sql_str(cat)}, {sql_str(sub)}, {price:.2f});"
    )

out.append("")
out.append("-- dim_customer (12 rows across multiple cities/states)")
for cid, cname, city, state, seg in customers:
    out.append(
        "INSERT INTO dim_customer (customer_id, customer_name, city, state, customer_segment) "
        f"VALUES ({sql_str(cid)}, {sql_str(cname)}, {sql_str(city)}, {sql_str(state)}, {sql_str(seg)});"
    )

out.append("")
out.append("-- fact_sales (40 transactions)")
for dk, pk, ck, qty, up, disc, total in facts:
    out.append(
        "INSERT INTO fact_sales (date_key, product_key, customer_key, quantity_sold, unit_price, discount_amount, total_amount) "
        f"VALUES ({dk}, {pk}, {ck}, {qty}, {up:.2f}, {disc:.2f}, {total:.2f});"
    )

OUT_SQL.write_text("\n".join(out), encoding="utf-8")
print(f"Generated: {OUT_SQL}")
