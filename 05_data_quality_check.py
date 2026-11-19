# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

rows = []
def check(name, passed, detail='', severity='error'):
    rows.append((name, bool(passed), str(detail), severity))
    print('PASS' if passed else 'FAIL', name, detail)

s = spark.table(T('silver_orders'))
total = s.count()
check('silver not empty', total > 0, total)
check('order_id unique', s.select('order_id').distinct().count() == total)
check('amounts positive', s.filter('amount <= 0').count() == 0)
check('dates in 2026', s.filter(F.col('order_date').between('2026-01-01', '2026-12-31')).count() == total)

q = spark.table(T('quarantine_orders')).count()
b = spark.table(T('bronze_orders')).count()
check('rejected rows under 25 percent', q / b < 0.25, f'{q}/{b}')

orph = s.join(spark.table(T('silver_customers')), 'customer_id', 'left_anti').count()
check('orders without customer under 10 percent', orph / total < 0.10, orph, 'warn')

(spark.createDataFrame(rows, 'check string, passed boolean, detail string, severity string')
    .withColumn('run_at', F.current_timestamp())
    .write.mode('append').saveAsTable(T('dq_results')))

failed = [r[0] for r in rows if not r[1] and r[3] == 'error']
if failed:
    raise Exception(f'Data quality failed: {failed}')

# COMMAND ----------

spark.sql("ALTER TABLE workspace.pipeline_demo.silver_orders DROP CONSTRAINT IF EXISTS positive_amount")
spark.sql("ALTER TABLE workspace.pipeline_demo.silver_orders ADD CONSTRAINT positive_amount CHECK (amount > 0)")

# COMMAND ----------

from pyspark.sql import functions as F

df_silver = spark.table("workspace.pipeline_demo.silver_orders")

freshness_row = df_silver.select(
    F.max("order_date").alias("latest_order_date"),
    F.datediff(F.current_date(), F.max("order_date")).alias("days_old")
).collect()[0]

latest_date = freshness_row["latest_order_date"]
days_old = freshness_row["days_old"]

print(f"Tanggal pesanan terbaru: {latest_date} | Umur data: {days_old} hari")

assert days_old is not None and days_old <= 45, (
    f"FRESHNESS CHECK FAILED: Data terbaru ({latest_date}) sudah berumur {days_old} hari (batas maksimal 45 hari)."
)

print("FRESHNESS CHECK PASSED: Data masih segar (<= 45 hari).")