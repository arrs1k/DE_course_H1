import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BUCKET = 'de-course'
MART_PATH = f"s3a://{BUCKET}/marts/fct_orders"


spark = (
    SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get('MINIO_ENDPOINT'))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get('MINIO_ROOT_USER'))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get('MINIO_ROOT_PASSWORD'))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

parser = argparse.ArgumentParser()
parser.add_argument("--load-date", required=True)
args = parser.parse_args()

load_date = datetime.strptime(args.load_date, "%Y-%m-%d").date()
start_date = load_date - timedelta(days=3)

df_int = (
    spark.read.parquet(f"s3a://{BUCKET}/intermediate/int_orders_with_payments")
    .filter((F.col("order_date") >= start_date) & (F.col("order_date") < load_date))
    .filter(~F.col("is_test_order"))
)

df_customers = spark.read.parquet(f"s3a://{BUCKET}/raw/customers")

df_fct = (
    df_int.alias("o")
    .join(df_customers.alias("c"), on="customer_id", how="left")
    .select(
        F.col("o.order_id"),
        F.col("o.customer_id"),
        F.col("c.city").alias("customer_city"),
        F.col("c.acquisition_channel"),
        F.col("o.ordered_at"),
        F.col("o.order_date"),
        F.col("o.order_status"),
        F.col("o.order_amount"),
        F.col("o.paid_amount"),
        F.col("o.net_paid_amount"),
        F.col("o.is_confirmed"),
        F.datediff(F.col("o.order_date"), F.to_date(F.col("c.registered_at"))).alias("days_since_registration"),
    )
)

(
    df_fct.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("order_date")
    .save(MART_PATH)
)

print(f"Сохранено: {MART_PATH}")

