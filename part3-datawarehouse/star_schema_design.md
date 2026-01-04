# Star Schema Design — FlexiMart (Part 3)

## Section 1: Schema Overview

### FACT TABLE: fact_sales
**Grain:** One row per product per order line item  
**Business Process:** Sales transactions

**Measures (Numeric Facts):**
- quantity_sold: Units sold in the line item
- unit_price: Price per unit at the time of sale
- discount_amount: Discount applied on the line
- total_amount: Final amount = (quantity_sold × unit_price) - discount_amount

**Foreign Keys:**
- date_key → dim_date
- product_key → dim_product
- customer_key → dim_customer

---

### DIMENSION TABLE: dim_date
**Purpose:** Time-based analysis and grouping  
**Type:** Conformed dimension (standard calendar attributes)

**Attributes:**
- date_key (PK): Surrogate key in YYYYMMDD format
- full_date: Actual calendar date
- day_of_week: Monday, Tuesday, etc.
- day_of_month: 1–31
- month: 1–12
- month_name: January, February, etc.
- quarter: Q1–Q4
- year: 2023, 2024, etc.
- is_weekend: TRUE/FALSE

---

### DIMENSION TABLE: dim_product
**Purpose:** Product master details for slicing revenue by product/category  
**Attributes:**
- product_key (PK): Surrogate key (AUTO_INCREMENT)
- product_id: Natural/business product id (e.g., P001)
- product_name: Product name
- category: Electronics/Fashion/Groceries
- subcategory: e.g., Smartphones, Footwear, Staples
- unit_price: Standard unit price

---

### DIMENSION TABLE: dim_customer
**Purpose:** Customer segmentation and geography analysis  
**Attributes:**
- customer_key (PK): Surrogate key (AUTO_INCREMENT)
- customer_id: Natural/business customer id (e.g., C001)
- customer_name: Full name
- city: City
- state: State
- customer_segment: Segment label (e.g., High/Mid/Low or Retail/Premium)

---

## Section 2: Design Decisions (≈150 words)

This warehouse uses a line-item grain in fact_sales because it preserves maximum detail: analysts can roll up to daily/monthly totals, drill down into products, or isolate customer-level behavior without losing information. Using surrogate keys (product_key, customer_key, date_key) improves performance and stability: business identifiers can change format or be reused, but surrogate keys remain consistent and compact for joins. The date dimension enables easy time intelligence (month, quarter, weekend flags) and supports drill-down (Year → Quarter → Month → Day) and roll-up across consistent calendar logic. The star schema simplifies analytical queries because the fact table stores measurable events while dimensions store descriptive attributes, allowing fast grouping and filtering (e.g., “Weekend Electronics revenue in Feb 2024 by city”). This separation reduces query complexity compared to operational schemas and aligns with standard OLAP patterns.

---

## Section 3: Sample Data Flow

**Source Transaction:**  
Order #101, Customer "John Doe", Product "Laptop", Qty: 2, Price: 50000

**Becomes in Data Warehouse:**  
fact_sales: {  
  date_key: 20240115,  
  product_key: 5,  
  customer_key: 12,  
  quantity_sold: 2,  
  unit_price: 50000,  
  discount_amount: 0,  
  total_amount: 100000  
}

dim_date: {date_key: 20240115, full_date: '2024-01-15', month: 1, quarter: 'Q1', ...}  
dim_product: {product_key: 5, product_name: 'Laptop', category: 'Electronics', ...}  
dim_customer: {customer_key: 12, customer_name: 'John Doe', city: 'Mumbai', ...}
