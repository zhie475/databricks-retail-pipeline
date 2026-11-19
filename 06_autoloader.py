# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

def run_autoloader():
    (spark.readStream.format('cloudFiles')
        .option('cloudFiles.format', 'csv')
        .option('header', 'true')
        .option('cloudFiles.inferColumnTypes', 'true')
        .option('cloudFiles.schemaLocation', f'{BASE}/_schemas/orders')
        .load(f'{BASE}/orders/')
        .withColumn('_ingested_at', F.current_timestamp())
        .withColumn('_source_file', F.col('_metadata.file_path'))
        .writeStream
        .option('checkpointLocation', f'{BASE}/_checkpoints/orders')
        .trigger(availableNow=True)
        .toTable(T('bronze_orders_incr'))
        .awaitTermination())

run_autoloader()
print(spark.table(T('bronze_orders_incr')).count())

# COMMAND ----------

import pandas as pd, numpy as np

rng = np.random.default_rng(99)
m = 20_000
new = pd.DataFrame({
    'order_id': rng.integers(185_000, 205_000, m),
    'customer_id': rng.integers(1, 21_000, m),
    'product': rng.choice(['laptop', 'Phone', 'MOUSE', 'keyboard', 'monitor', 'cable'], m),
    'city': rng.choice(['Jakarta', 'Pekanbaru', 'Medan', 'Bandung'], m),
    'amount': rng.normal(95, 60, m).round(2),
    'order_date': (pd.to_datetime('2026-04-01') + pd.to_timedelta(rng.integers(0, 30, m), unit='D')).strftime('%Y-%m-%d')
})
new.to_csv(f'{BASE}/orders/orders_2026q2.csv', index=False)

run_autoloader()
print(spark.table(T('bronze_orders_incr')).count())

# COMMAND ----------

good, bad = clean_orders(spark.table(T('bronze_orders_incr')))
good.createOrReplaceTempView('updates')
silver = T('silver_orders')
before_count = spark.table(silver).count()
target = spark.table(silver)

insert_count = (
    good.select('order_id')
    .join(target.select('order_id'), 'order_id', 'left_anti')
    .count()
)
update_count = (
    good.select('order_id')
    .join(target.select('order_id'), 'order_id', 'inner')
    .count()
)
print("Records to insert:", insert_count)
print("Records to update:", update_count)

spark.sql(f'''
MERGE INTO {silver} t USING updates u ON t.order_id = u.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
''')

print(spark.table(silver).count())
after_count = spark.table(silver).count()
print("Silver before MERGE:", before_count)
print("Silver after MERGE:", after_count)
print("Expected Silver count:", before_count + insert_count)

duplicates = (
    spark.table(silver).count()
    - spark.table(silver).select('order_id').distinct().count()
)
print("Duplicate order IDs:", duplicates)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.pipeline_demo.silver_orders;
# MAGIC SELECT COUNT(*) FROM workspace.pipeline_demo.silver_orders VERSION AS OF 0;