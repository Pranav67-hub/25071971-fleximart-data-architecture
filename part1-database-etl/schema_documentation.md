# FlexiMart Database Schema Documentation (Part 1)

## 1) Entity–Relationship Description (Text Format)

### ENTITY: customers
**Purpose:** Stores unique customer master data used for placing orders.  
**Attributes:**
- customer_id (PK): Surrogate key, auto-increment integer
- first_name: Customer first name (NOT NULL)
- last_name: Customer last name (NOT NULL)
- email: Unique customer email (UNIQUE, NOT NULL)
- phone: Standardized phone number (nullable)
- city: Customer city (nullable)
- registration_date: Customer registration date (nullable)

### ENTITY: products
**Purpose:** Stores product master data for the catalog.  
**Attributes:**
- product_id (PK): Surrogate key, auto-increment integer
- product_name: Product name (NOT NULL)
- category: Standardized category name (NOT NULL)
- price: Product price (NOT NULL)
- stock_quantity: Inventory count (default 0)

### ENTITY: orders
**Purpose:** Stores order headers (one order per purchase event).  
**Attributes:**
- order_id (PK): Surrogate key, auto-increment integer
- customer_id (FK → customers.customer_id): Links order to a customer (NOT NULL)
- order_date: Date of the order (NOT NULL)
- total_amount: Total order value (NOT NULL)
- status: Order status (default 'Pending')

### ENTITY: order_items
**Purpose:** Stores order line items (products within each order).  
**Attributes:**
- order_item_id (PK): Surrogate key, auto-increment integer
- order_id (FK → orders.order_id): Links item to an order (NOT NULL)
- product_id (FK → products.product_id): Links item to a product (NOT NULL)
- quantity: Units purchased (NOT NULL)
- unit_price: Price per unit at time of purchase (NOT NULL)
- subtotal: quantity × unit_price (NOT NULL)

## 2) Relationships
- **customers (1) → orders (M):** One customer can place many orders.
- **orders (1) → order_items (M):** One order can contain many line items.
- **products (1) → order_items (M):** One product can appear in many order items.

---

## 3) Normalization Explanation (3NF) (200–250 words)

This database design is in Third Normal Form (3NF) because each table models a single entity type and every non-key attribute depends only on the primary key, without partial or transitive dependencies. In the **customers** table, the functional dependency is `customer_id → {first_name, last_name, email, phone, city, registration_date}`. In **products**, `product_id → {product_name, category, price, stock_quantity}`. In **orders**, `order_id → {customer_id, order_date, total_amount, status}`. In **order_items**, `order_item_id → {order_id, product_id, quantity, unit_price, subtotal}`.

There are no partial dependencies because every table uses a single-column surrogate primary key. There are also no transitive dependencies inside a table; for example, customer details are not stored in **orders**, and product details are not stored in **order_items**. Instead, **orders** references customers via a foreign key, and **order_items** references products via a foreign key. This prevents update anomalies (e.g., changing a customer email in one place only), insert anomalies (e.g., being unable to add a product without an order), and delete anomalies (e.g., deleting an order does not delete the product master record). As a result, the schema is normalized, consistent, and supports reliable joins for analytics.

---

## 4) Sample Data Representation (Examples)

### customers (sample)
| customer_id | first_name | last_name | email | phone | city | registration_date |
|---:|---|---|---|---|---|---|
| 1 | Rahul | Sharma | rahul.sharma@gmail.com | +91-9876543210 | Bangalore | 2023-01-15 |
| 2 | Priya | Patel | priya.patel@yahoo.com | +91-9988776655 | Mumbai | 2023-02-20 |

### products (sample)
| product_id | product_name | category | price | stock_quantity |
|---:|---|---|---:|---:|
| 1 | Samsung Galaxy S21 | Electronics | 45999.00 | 150 |
| 2 | Nike Running Shoes | Fashion | 3499.00 | 80 |

### orders (sample)
| order_id | customer_id | order_date | total_amount | status |
|---:|---:|---|---:|---|
| 1 | 1 | 2024-01-15 | 45999.00 | Completed |
| 2 | 2 | 2024-01-16 | 5998.00 | Completed |

### order_items (sample)
| order_item_id | order_id | product_id | quantity | unit_price | subtotal |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 1 | 1 | 45999.00 | 45999.00 |
| 2 | 2 | 4 | 2 | 2999.00 | 5998.00 |
