## Postman Data Engineering Round 1 (2022)
The goal of this [problem statement](https://drive.google.com/drive/folders/1X3qomdbjWU1oOTbBvxchTzjLMAwYBWFT) is to store a large csv file into a Relational Database (In my case, I have used [mysql](https://www.mysql.com/products/workbench/)).

## Notes and Quick Start
1. I had manually downloaded the data [products.csv.gz](https://doc-14-08-docs.googleusercontent.com/docs/securesc/6pr2dept42cvgih3lgfsarcv6d7624me/oiech7ac2a859sbt6hait2rd7o25ddno/1652593500000/02053056633443036242/11570081877556574219Z/11ACp03VCQY5NElctMq7F5zn23jKrqTZI?e=download&nonce=14hmecqrl5ajq&user=11570081877556574219Z&hash=8qvgide0lvb5to682a4vudlaurn361v8) file and saved products.csv in [data](./data) folder. For your reference I have put [products.csv](./data/products.csv) in data folder.

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

6. The database should be populated with the products info (see dabaase schema [here](#database-schema))

## Database Schema