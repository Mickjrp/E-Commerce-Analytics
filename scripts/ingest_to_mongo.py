import os
from pathlib import Path
import pandas as pd
from pymongo import MongoClient

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
RAW_DIR = DATA_DIR / "raw"

MONGO_USER = os.getenv("MONGO_USER", "ecom_admin")
MONGO_PASS = os.getenv("MONGO_PASS", "ecom_admin_pass")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
DB_NAME    = os.getenv("MONGO_DB", "ecom")

client = MongoClient(f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/")
db = client[DB_NAME]

def load_csv_to_mongo(filename, collection, index_field=None):
    path = RAW_DIR / filename
    print(f"Loading {filename} -> {collection} ...")
    df = pd.read_csv(path)
    db[collection].drop()
    db[collection].insert_many(df.to_dict("records"))
    if index_field:
        db[collection].create_index(index_field)
    print(f"Inserted {db[collection].count_documents({})} records into {collection}")

load_csv_to_mongo("olist_customers_dataset.csv", "customers", "customer_id")
load_csv_to_mongo("olist_orders_dataset.csv", "orders", "order_id")
load_csv_to_mongo("olist_order_items_dataset.csv", "items", "order_id")
load_csv_to_mongo("olist_order_reviews_dataset.csv", "reviews", "order_id")
load_csv_to_mongo("olist_geolocation_dataset.csv", "geolocation", "geolocation_zip_code_prefix")
load_csv_to_mongo("olist_order_payments_dataset.csv", "payments", "order_id")
load_csv_to_mongo("olist_products_dataset.csv", "products", "product_id")
load_csv_to_mongo("olist_sellers_dataset.csv", "sellers", "seller_id")
load_csv_to_mongo("product_category_name_translation.csv", "category_translation", "product_category_name")

print("Ingestion (ALL datasets) complete!")
