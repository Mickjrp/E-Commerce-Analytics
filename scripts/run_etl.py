import subprocess
import sys

steps = [
    ("Ingestion → MongoDB", "scripts/ingest_to_mongo.py"),
    ("Transform & Load → PostgreSQL", "scripts/transform_load_postgres.py"),
    ("Analytics → RFM Segmentation", "scripts/analytics_rfm.py"),
]

for name, script in steps:
    print(f"\nRunning step: {name}")
    try:
        subprocess.run([sys.executable, script], check=True)
        print(f"Done: {name}")
    except subprocess.CalledProcessError as e:
        print(f"Failed at {name}")
        sys.exit(1)

print("\nETL Pipeline finished successfully!")
