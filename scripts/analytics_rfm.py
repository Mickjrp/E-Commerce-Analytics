import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Float, String, DateTime, Numeric

# =========================
# Config
# =========================
PG_USER   = os.getenv("PG_USER", "ecom_user")
PG_PASS   = os.getenv("PG_PASS", "ecom_pass")
PG_HOST   = os.getenv("PG_HOST", "postgres")
PG_PORT   = int(os.getenv("PG_PORT", "5432"))
PG_DB     = os.getenv("PG_DB", "ecom_dw")
PG_SCHEMA = os.getenv("PG_SCHEMA", "ecom")

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    future=True
)

# =========================
# Build base table for RFM (customer_unique_id level)
# =========================
SQL = f"""
WITH pay AS (
  SELECT
    op.order_id,
    SUM(op.payment_value)::numeric(18,2) AS order_value
  FROM "{PG_SCHEMA}".order_payments op
  GROUP BY op.order_id
),
ord AS (
  SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.purchased_at
  FROM "{PG_SCHEMA}".orders o
),
base AS (
  SELECT
    c.customer_unique_id,
    ord.order_id,
    ord.order_status,
    ord.purchased_at,
    COALESCE(pay.order_value, 0)::numeric(18,2) AS order_value
  FROM ord
  JOIN "{PG_SCHEMA}".customers c
    ON c.customer_id = ord.customer_id
  LEFT JOIN pay
    ON pay.order_id = ord.order_id
)
SELECT * FROM base;
"""

with engine.connect() as conn:
    df = pd.read_sql(SQL, conn)

# =========================
# Clean & Filter
# =========================
df["order_status"] = df["order_status"].fillna("").str.lower()
df = df[df["order_status"] != "canceled"].copy()

df["purchased_at"] = pd.to_datetime(df["purchased_at"], errors="coerce")
df["order_value"]  = pd.to_numeric(df["order_value"], errors="coerce").fillna(0)

snapshot_date = df["purchased_at"].max()
if pd.isna(snapshot_date):
    raise ValueError("No purchased_at data found for RFM calculation.")

# =========================
# RFM
# =========================
rfm = (
    df.groupby("customer_unique_id", as_index=False)
      .agg(
          last_purchase=("purchased_at", "max"),
          frequency=("order_id", "nunique"),
          monetary=("order_value", "sum")
      )
)
rfm["recency_days"] = (snapshot_date - pd.to_datetime(rfm["last_purchase"])).dt.days.astype("Int64")

def quantile_score(s: pd.Series, reverse: bool = False) -> pd.Series:
    r = s.rank(method="first")
    if reverse:
        r = -r
    try:
        return pd.qcut(r, 5, labels=[1,2,3,4,5]).astype(int)
    except ValueError:
        return pd.cut(r, bins=5, labels=[1,2,3,4,5], include_lowest=True).astype(int)

rfm["r_score"] = quantile_score(rfm["recency_days"], reverse=True)
rfm["f_score"] = quantile_score(rfm["frequency"],   reverse=False)
rfm["m_score"] = quantile_score(rfm["monetary"],    reverse=False)
rfm["rfm_score"] = rfm["r_score"].astype(str) + rfm["f_score"].astype(str) + rfm["m_score"].astype(str)

def segment_row(r, f, m):
    if r >= 4 and f >= 4 and m >= 4:
        return "Champions"
    if r >= 4 and f >= 3:
        return "Loyal"
    if r >= 3 and f >= 3 and m >= 3:
        return "Potential Loyalist"
    if r <= 2 and f >= 3 and m >= 3:
        return "At Risk"
    if f <= 2 and m <= 2 and r <= 2:
        return "Hibernating"
    if r >= 3 and (f <= 2 or m <= 2):
        return "Recent"
    return "Others"

rfm["segment"] = rfm.apply(lambda x: segment_row(x["r_score"], x["f_score"], x["m_score"]), axis=1)
rfm["snapshot_date"] = snapshot_date

out = rfm[[
    "customer_unique_id", "recency_days", "frequency", "monetary",
    "r_score", "f_score", "m_score", "rfm_score", "segment", "snapshot_date"
]].copy()

# =========================
# Write to Postgres
# =========================
DDL = f"""
DROP TABLE IF EXISTS "{PG_SCHEMA}".customer_segments;

CREATE TABLE "{PG_SCHEMA}".customer_segments (
  customer_unique_id TEXT PRIMARY KEY,
  recency_days      INT,
  frequency         INT,
  monetary          NUMERIC(18,2),
  r_score           INT,
  f_score           INT,
  m_score           INT,
  rfm_score         TEXT,
  segment           TEXT,
  snapshot_date     TIMESTAMP
);
"""

with engine.begin() as conn:
    conn.execute(text(DDL))
    out.to_sql(
        "customer_segments",
        con=conn,
        schema=PG_SCHEMA,
        if_exists="append",
        index=False,
        dtype={
            "customer_unique_id": String(64),
            "recency_days": Integer(),
            "frequency": Integer(),
            "monetary": Numeric(18, 2),
            "r_score": Integer(),
            "f_score": Integer(),
            "m_score": Integer(),
            "rfm_score": String(16),
            "segment": String(64),
            "snapshot_date": DateTime()
        }
    )
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS ix_seg_segment ON "{PG_SCHEMA}".customer_segments (segment);'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS ix_seg_snapshot ON "{PG_SCHEMA}".customer_segments (snapshot_date);'))

print("RFM segmentation complete → ecom.customer_segments (PK = customer_unique_id)")
