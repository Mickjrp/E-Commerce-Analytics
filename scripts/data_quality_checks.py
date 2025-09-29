import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

# Config
PG_USER   = os.getenv("PG_USER", "ecom_user")
PG_PASS   = os.getenv("PG_PASS", "ecom_pass")
PG_HOST   = os.getenv("PG_HOST", "localhost")
PG_PORT   = int(os.getenv("PG_PORT", "5432"))
PG_DB     = os.getenv("PG_DB", "ecom_dw")
PG_SCHEMA = os.getenv("PG_SCHEMA", "ecom")

DATA_DIR = Path(os.getenv("DATA_DIR", "data")).resolve()
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

def q(sql): 
    return pd.read_sql(sql, engine)

def dq_row_count(table):
    n = q(f'SET search_path TO "{PG_SCHEMA}"; SELECT COUNT(*) AS n FROM {table}')["n"].iloc[0]
    return {"check": f"{table} row_count > 0", "status": "PASS" if n > 0 else "FAIL", "info": f"rows={n}"}

def dq_unique(table, col):
    dup = q(f'SET search_path TO "{PG_SCHEMA}"; '
            f'SELECT COUNT(*) AS d FROM (SELECT {col}, COUNT(*) c FROM {table} GROUP BY {col} HAVING COUNT(*)>1) t')["d"].iloc[0]
    return {"check": f"{table}.{col} unique", "status": "PASS" if dup == 0 else "FAIL", "info": f"duplicates={dup}"}

def dq_not_null(table, col):
    nn = q(f'SET search_path TO "{PG_SCHEMA}"; SELECT COUNT(*) AS x FROM {table} WHERE {col} IS NULL')["x"].iloc[0]
    return {"check": f"{table}.{col} not null", "status": "PASS" if nn == 0 else "FAIL", "info": f"nulls={nn}"}

def dq_non_negative(table, col):
    bad = q(f'SET search_path TO "{PG_SCHEMA}"; SELECT COUNT(*) AS x FROM {table} WHERE {col} < 0')["x"].iloc[0]
    return {"check": f"{table}.{col} >= 0", "status": "PASS" if bad == 0 else "FAIL", "info": f"negatives={bad}"}

def dq_status_in(table, col, allowed):
    alist = ",".join([f"'{x}'" for x in allowed])
    bad = q(f'SET search_path TO "{PG_SCHEMA}"; SELECT COUNT(*) AS x FROM {table} WHERE {col} IS NOT NULL AND {col} NOT IN ({alist})')["x"].iloc[0]
    return {"check": f"{table}.{col} allowed_values", "status": "PASS" if bad == 0 else "FAIL", "info": f"out_of_set={bad}"}

def dq_date_order():
    # delivered_to_customer_at (Should >= purchased_at)
    bad = q(f'''
      SET search_path TO "{PG_SCHEMA}";
      SELECT COUNT(*) AS x
      FROM orders
      WHERE delivered_to_customer_at IS NOT NULL
        AND purchased_at IS NOT NULL
        AND delivered_to_customer_at < purchased_at
    ''')["x"].iloc[0]
    return {"check": "orders delivered_at >= purchased_at", "status": "PASS" if bad == 0 else "FAIL", "info": f"violations={bad}"}

def main():
    results = []
    # 1) row counts
    for t in ["orders","order_items","customers","products","sellers","order_payments"]:
        results.append(dq_row_count(t))
    # 2) primary keys uniqueness
    results += [
        dq_unique("orders","order_id"),
        dq_unique("customers","customer_id"),
        dq_unique("products","product_id"),
        dq_unique("sellers","seller_id"),
    ]
    # 3) not null
    results += [
        dq_not_null("orders","order_id"),
        dq_not_null("orders","customer_id"),
        dq_not_null("customers","customer_id"),
    ]
    # 4) Currency not negative
    results += [
        dq_non_negative("orders","order_value"),
        dq_non_negative("order_payments","payment_value_total"),
    ]
    # 5) Order status
    allowed = ["delivered","shipped","invoiced","processing","unavailable","canceled","created","approved"]
    results.append(dq_status_in("orders","order_status", allowed))
    # 6) Date
    results.append(dq_date_order())

    df = pd.DataFrame(results)
    out = PROCESSED_DIR / "quality_report.csv"
    df.to_csv(out, index=False, encoding="utf-8")

    
    # Summary
    fails = (df["status"]=="FAIL").sum()
    print(df)
    print(f"\nData Quality report saved: {out}")
    if fails > 0:
        print(f"FAIL checks: {fails}")

if __name__ == "__main__":
    main()
