import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BUCKET = 'de-course'
INT_PATH = f"s3a://{BUCKET}/intermediate/int_orders_with_payments"


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

df_orders = (
    spark.read.parquet(f"s3a://{BUCKET}/staging/stg_orders")
    .filter((F.col("order_date") >= start_date) & (F.col("order_date") < load_date))
)

df_payments = spark.read.parquet(f"s3a://{BUCKET}/staging/stg_payments")

df_pay_agg = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum(F.when((F.col("payment_status") == "success") & (~F.col("is_refund")), F.col("amount")).otherwise(0)).alias("paid_amount"),
        F.sum(F.when(F.col("is_refund"), F.col("amount")).otherwise(0)).alias("refund_amount"),
        F.sum(F.when(F.col("payment_status") == "success", F.col("amount")).otherwise(0)).alias("net_paid_amount"),
        F.count("*").alias("payments_count"),
        F.sum(F.when(F.col("payment_status") == "success", 1).otherwise(0)).alias("successful_payments_count"),
        F.sum(F.when(F.col("payment_status") == "failed", 1).otherwise(0)).alias("failed_payments_count"),
        F.min(F.when((F.col("payment_status") == "success") & (~F.col("is_refund")), F.col("paid_at"))).alias("first_paid_at"),
        F.max(F.when((F.col("payment_status") == "success") & (~F.col("is_refund")), F.col("paid_at"))).alias("last_paid_at"),
    )
)

df_int = (
    df_orders.alias("o")
    .join(df_pay_agg.alias("p"), on="order_id", how="left")
    .select(
        F.col("o.order_id"),
        F.col("o.customer_id"),
        F.col("o.ordered_at"),
        F.col("o.order_date"),
        F.col("o.order_status"),
        F.col("o.order_amount"),
        F.col("o.is_test_order"),
        F.coalesce(F.col("p.paid_amount"), F.lit(0)).cast("decimal(12,2)").alias("paid_amount"),
        F.coalesce(F.col("p.refund_amount"), F.lit(0)).cast("decimal(12,2)").alias("refund_amount"),
        F.coalesce(F.col("p.net_paid_amount"), F.lit(0)).cast("decimal(12,2)").alias("net_paid_amount"),
        F.coalesce(F.col("p.payments_count"), F.lit(0)).alias("payments_count"),
        F.coalesce(F.col("p.successful_payments_count"), F.lit(0)).alias("successful_payments_count"),
        F.coalesce(F.col("p.failed_payments_count"), F.lit(0)).alias("failed_payments_count"),
        F.col("p.first_paid_at"),
        F.col("p.last_paid_at"),
        (F.coalesce(F.col("p.net_paid_amount"), F.lit(0)) > 0).alias("is_confirmed"),
    )
)

(
    df_int.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("order_date")
    .save(INT_PATH)
)

print(f"Сохранено: {INT_PATH}")
