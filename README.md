## Postman Data Engineering Round 1 (2022)
The goal of this [problem statement](https://drive.google.com/drive/folders/1X3qomdbjWU1oOTbBvxchTzjLMAwYBWFT) is to store a large csv file into a Relational Database (In my case, I have used [mysql](https://www.mysql.com/products/workbench/)).

## Notes and Quick Start
1. I had manually downloaded the data products.csv.gz file from [here](https://drive.google.com/drive/folders/1X3qomdbjWU1oOTbBvxchTzjLMAwYBWFT) file and saved products.csv from the zip in [data](./data) folder. For your reference I have put [products.csv](./data/products.csv) in data folder.

2. Use [python >= 3.7.6](https://www.python.org/downloads/release/python-376/) and [mysql](https://www.mysql.com/products/workbench/) database

3. Install dependencies
```console
python -m pip install -r requirements.txt
```

4. Update database credentials in [db.toml](./db.toml) file

5. Run the script
```console
python ./run.py
```

6. The database should be populated with the products info (see database schema [here](#database-schema))

## Flow Diagram
The following diagram shows roughly what tasks are running in the script. I have used [prefect](https://docs.prefect.io/) to orchestrate these tasks. The flow is ran using `LocalDaskExecutor`.

1. load_csv_to_stg_products_taskfn - loads the csv path to stg_products table (chunk by chunk)

2. update_skus_table_taskfn - updates skus table with new (if any) skus available in stg_products table

3. update_names_table_taskfn - updates names table with new (if any) names available in stg_products table

4. update_products_table_taskfn - updates products table with latest products info

5. update_by_name_no_of_products_table_taskfn - updates the aggregate table `by_name_no_of_products` with latest values

![](./flow_diagram.png)

## Database Schema
You can see table details as code [here](./db/tables.py).

![](./db_schema.png)

## Points to achieve
- [x] Your code should follow concept of OOPS
- [x] Support for regular non-blocking parallel ingestion of the given file into a table. Consider thinking about the scale of what should happen if the file is to be processed in 2 mins.
- [x] Support for updating existing products in the table based on `sku` as the primary key. (Yes, we know about the kind of data in the file. You need to find a workaround for it)
- [x] All product details are to be ingested into a single table
- [x] An aggregated table on above rows with `name` and `no. of products` as the columns

## Table Row Counts
| table_name | counts |
| ---------- | ------ |
stg_products | 500,000
skus | 466,693
names | 222,024
products | 499,994
by_name_no_of_products | 222,024

Query to get row counts-

```sql
SELECT
	'stg_products' table_name,
	COUNT(1) count
FROM products.stg_products
UNION
SELECT
	'skus' table_name,
	COUNT(1) count
FROM products.skus
UNION
SELECT
	'names' table_name,
	COUNT(1) count
FROM products.names
UNION
SELECT
	'products' table_name,
	COUNT(1) count
FROM products.products
UNION
SELECT
	'by_name_no_of_products' table_name,
	COUNT(1) count
FROM products.by_name_no_of_products ;
```
