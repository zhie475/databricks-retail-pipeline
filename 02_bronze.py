# Databricks notebook source
# Membuat widget parameter untuk lokasi folder sumber utama
dbutils.widgets.text(
    "landing_folder",
    "/Volumes/workspace/pipeline_demo/landing",
    "Landing Folder Path"
)

# Mengambil nilai folder yang aktif (baik dari default maupun dari Job)
landing_folder = dbutils.widgets.get("landing_folder")
print(f"Membaca data dari folder: {landing_folder}")

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

BASE = dbutils.widgets.get("landing_folder")

(spark.read.option('header', True).option('inferSchema', True).csv(f'{BASE}/orders/')
    .withColumn('_ingested_at', F.current_timestamp())
    .withColumn('_source_file', F.col('_metadata.file_path'))
    .write.mode('overwrite').saveAsTable(T('bronze_orders')))

(spark.read.json(f'{BASE}/customers/')
    .withColumn('_ingested_at', F.current_timestamp())
    .write.mode('overwrite').saveAsTable(T('bronze_customers')))