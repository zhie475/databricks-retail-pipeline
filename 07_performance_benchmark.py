# Databricks notebook source
import time
from pyspark.sql import functions as F

# 1. Membaca dataset awal dari silver_orders (~200K baris)
df_200k = spark.table("workspace.pipeline_demo.silver_orders")
count_200k = df_200k.count()

# 2. Membuat simulasi dataset 10x lipat (~2 juta baris)
df_2m = df_200k.crossJoin(spark.range(10).withColumnRenamed("id", "batch_id"))
count_2m = df_2m.count()

# Fungsi untuk mengukur durasi kueri agregasi
def benchmark_aggregation(df, row_count, label):
    start_time = time.time()
    _ = (
        df.groupBy("product", "city")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("avg_order_value")
        )
        .orderBy(F.desc("total_revenue"))
        .collect()
    )
    elapsed = time.time() - start_time
    print(f"[{label}] Baris: {row_count:,} | Waktu agregasi: {elapsed:.3f} detik")
    return elapsed

# 3. Jalankan perbandingan performa
print("=== BENCHMARK PERFORMA AGREGASI SPARK ===")
t_200k = benchmark_aggregation(df_200k, count_200k, "Dataset 200K")
t_2m = benchmark_aggregation(df_2m, count_2m, "Dataset 2M  ")

ratio = t_2m / t_200k if t_200k > 0 else 0
print(f"\nAnalisis: Volume data naik 10x lipat, waktu komputasi hanya naik {ratio:.2f}x.")