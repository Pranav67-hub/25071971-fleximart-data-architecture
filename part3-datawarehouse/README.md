# Part 3 — Data Warehouse & Analytics (Star Schema)

## What this part contains
- `warehouse_schema.sql`: Creates star schema tables in `fleximart_dw`
- `warehouse_data.sql`: Inserts required minimum data volumes (dims + 40 fact rows)
- `analytics_queries.sql`: OLAP queries for drill-down, product performance, customer segmentation
- `star_schema_design.md`: Documentation of schema, design choices, sample flow
- `generate_warehouse_data.py`: Deterministic generator for `warehouse_data.sql` (seeded)

## Run Instructions (Windows PowerShell friendly)
Create database:
```powershell
docker exec -i fleximart-mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS fleximart_dw; CREATE DATABASE fleximart_dw;"
