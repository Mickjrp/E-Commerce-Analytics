import os
import numpy as np
import pandas as pd
from pathlib import Path
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Float, String, DateTime

# ==== CONFIG ====
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Mongo
MONGO_USER = os.getenv("MONGO_USER", "ecom_admin")
MONGO_PASS = os.getenv("MONGO_PASS", "ecom_admin_pass")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DB   = os.getenv("MONGO_DB", "ecom")

# Postgres
PG_USER   = os.getenv("PG_USER", "ecom_user")
PG_PASS   = os.getenv("PG_PASS", "ecom_pass")
PG_HOST   = os.getenv("PG_HOST", "localhost")
PG_PORT   = int(os.getenv("PG_PORT", "5432"))
PG_DB     = os.getenv("PG_DB", "ecom_dw")
PG_SCHEMA = os.getenv("PG_SCHEMA", "ecom")

# ==== CONNECT ====
mongo = MongoClient(f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/")
mdb = mongo[MONGO_DB]
engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

def df_from(coll, fields=None):
    cur = mdb[coll].find({}, fields or {})
    df  = pd.DataFrame(list(cur))
    if "_id" in df: df.drop(columns="_id", inplace=True, errors="ignore")
    return df

print("Reading from MongoDB ...")
cust   = df_from("customers")
orders = df_from("orders")
items  = df_from("items")
reviews= df_from("reviews")
geo    = df_from("geolocation")
pay    = df_from("payments")
prod   = df_from("products")
sell   = df_from("sellers")
cat_tr = df_from("category_translation")

# ==== Clean/Type ====
to_dt = lambda s: pd.to_datetime(s, errors="coerce")
for c in ["order_purchase_timestamp","order_approved_at",
          "order_delivered_carrier_date","order_delivered_customer_date",
          "order_estimated_delivery_date"]:
    if c in orders: orders[c]=to_dt(orders[c])
if "shipping_limit_date" in items: items["shipping_limit_date"]=to_dt(items["shipping_limit_date"])
if "review_creation_date" in reviews: reviews["review_creation_date"]=to_dt(reviews["review_creation_date"])
if "review_answer_timestamp" in reviews: reviews["review_answer_timestamp"]=to_dt(reviews["review_answer_timestamp"])

# ==== Products + Category translation ====
prod = prod.merge(cat_tr, how="left",
                  left_on="product_category_name",
                  right_on="product_category_name")
# rename translation to english_name
if "product_category_name_english" in prod:
    prod.rename(columns={"product_category_name_english":"category_en"}, inplace=True)

dim_products = prod[[
    "product_id","product_category_name","category_en",
    "product_weight_g","product_length_cm","product_height_cm","product_width_cm"
]]

# ==== Sellers ====
dim_sellers = sell[["seller_id","seller_zip_code_prefix","seller_city","seller_state"]]

# ==== Customers + Geo enrich ====
geo_small = geo[["geolocation_zip_code_prefix","geolocation_city","geolocation_state"]].drop_duplicates(
    subset=["geolocation_zip_code_prefix"]
)
dim_customers = cust.merge(
    geo_small,
    how="left",
    left_on="customer_zip_code_prefix",
    right_on="geolocation_zip_code_prefix",
)
dim_customers = dim_customers.rename(columns={
    "customer_id":"customer_id",
    "customer_unique_id":"customer_unique_id",
    "customer_zip_code_prefix":"zip_prefix",
    "customer_city":"city",
    "customer_state":"state",
    "geolocation_city":"geo_city",
    "geolocation_state":"geo_state",
})
dim_customers = dim_customers[["customer_id","customer_unique_id","zip_prefix","city","state","geo_city","geo_state"]]

# ==== Items helpers ====
items["item_total"] = items[["price","freight_value"]].sum(axis=1)

# ==== Payments (aggregate per order) ====
p_main = (pay.sort_values(["order_id","payment_value"], ascending=[True, False])
            .groupby("order_id", as_index=False)
            .first()[["order_id","payment_type","payment_installments","payment_value"]])
p_sum  = pay.groupby("order_id", as_index=False).agg(
            payment_value_total=("payment_value","sum"),
            payment_methods=("payment_type","nunique")
        )
fact_payments = p_main.merge(p_sum, on="order_id", how="left")

# ==== Orders enriched ====
agg_items = items.groupby("order_id", as_index=False).agg(
    items_count=("order_item_id","count"),
    product_count=("product_id","nunique"),
    freight_total=("freight_value","sum"),
    item_value=("price","sum"),
    order_value=("item_total","sum")
)
orders_enriched = orders.rename(columns={
    "order_id":"order_id",
    "customer_id":"customer_id",
    "order_status":"order_status",
    "order_purchase_timestamp":"purchased_at",
    "order_approved_at":"approved_at",
    "order_delivered_carrier_date":"delivered_to_carrier_at",
    "order_delivered_customer_date":"delivered_to_customer_at",
    "order_estimated_delivery_date":"estimated_delivery_at"
})[["order_id","customer_id","order_status","purchased_at","approved_at",
    "delivered_to_carrier_at","delivered_to_customer_at","estimated_delivery_at"]]

fact_orders = (orders_enriched
               .merge(agg_items, on="order_id", how="left")
               .merge(fact_payments, on="order_id", how="left"))

# ==== Save processed files (Parquet) ====
dim_customers.to_parquet(PROCESSED_DIR/"dim_customers.parquet", index=False)
dim_products.to_parquet(PROCESSED_DIR/"dim_products.parquet", index=False)
dim_sellers.to_parquet(PROCESSED_DIR/"dim_sellers.parquet", index=False)
fact_orders.to_parquet(PROCESSED_DIR/"fact_orders.parquet", index=False)
items.to_parquet(PROCESSED_DIR/"fact_order_items.parquet", index=False)
fact_payments.to_parquet(PROCESSED_DIR/"fact_payments.parquet", index=False)

print(f"Saved processed parquet files to {PROCESSED_DIR}")

# ==== Load to Postgres ====
dtype_products = {
    "product_id": String(64),
    "product_category_name": String(255),
    "category_en": String(255),
    "product_weight_g": Float(),
    "product_length_cm": Float(),
    "product_height_cm": Float(),
    "product_width_cm": Float(),
}
dtype_sellers = {
    "seller_id": String(64),
    "seller_zip_code_prefix": Integer(),
    "seller_city": String(255),
    "seller_state": String(8),
}
dtype_customers = {
    "customer_id": String(64),
    "customer_unique_id": String(64),
    "zip_prefix": Integer(),
    "city": String(255),
    "state": String(8),
    "geo_city": String(255),
    "geo_state": String(8),
}
dtype_orders = {
    "order_id": String(64),
    "customer_id": String(64),
    "order_status": String(32),
    "purchased_at": DateTime(),
    "approved_at": DateTime(),
    "delivered_to_carrier_at": DateTime(),
    "delivered_to_customer_at": DateTime(),
    "estimated_delivery_at": DateTime(),
    "items_count": Integer(),
    "product_count": Integer(),
    "freight_total": Float(),
    "item_value": Float(),
    "order_value": Float(),
    "payment_type": String(64),
    "payment_installments": Integer(),
    "payment_value": Float(),
    "payment_value_total": Float(),
    "payment_methods": Integer(),
}
dtype_items = {
    "order_id": String(64),
    "order_item_id": Integer(),
    "product_id": String(64),
    "seller_id": String(64),
    "shipping_limit_date": DateTime(),
    "price": Float(),
    "freight_value": Float(),
    "item_total": Float(),
}
dtype_payments = {
    "order_id": String(64),
    "payment_type": String(64),
    "payment_installments": Integer(),
    "payment_value": Float(),
    "payment_value_total": Float(),
    "payment_methods": Integer(),
}

with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{PG_SCHEMA}"'))
    conn.execute(text(f'SET search_path TO "{PG_SCHEMA}"'))

    dim_customers.to_sql("customers", engine, schema=PG_SCHEMA, if_exists="replace", index=False, dtype=dtype_customers)
    dim_products.to_sql("products", engine, schema=PG_SCHEMA, if_exists="replace", index=False, dtype=dtype_products)
    dim_sellers.to_sql("sellers", engine, schema=PG_SCHEMA, if_exists="replace", index=False, dtype=dtype_sellers)
    fact_orders.to_sql("orders", engine, schema=PG_SCHEMA, if_exists="replace", index=False, dtype=dtype_orders)
    items.to_sql("order_items", engine, schema=PG_SCHEMA, if_exists="replace", index=False, dtype=dtype_items)
    fact_payments.to_sql("order_payments", engine, schema=PG_SCHEMA, if_exists="replace", index=False, dtype=dtype_payments)

with engine.begin() as conn:
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS ix_orders_order_id ON "{PG_SCHEMA}".orders (order_id)'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS ix_orders_customer_id ON "{PG_SCHEMA}".orders (customer_id)'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS ix_items_order_id ON "{PG_SCHEMA}".order_items (order_id)'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS ix_products_product_id ON "{PG_SCHEMA}".products (product_id)'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS ix_sellers_seller_id ON "{PG_SCHEMA}".sellers (seller_id)'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS ix_payments_order_id ON "{PG_SCHEMA}".order_payments (order_id)'))

print("Transform & Load (enriched) complete.")


