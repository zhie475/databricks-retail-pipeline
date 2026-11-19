# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

so, sc = T('silver_orders'), T('silver_customers')

spark.sql(f'''
CREATE OR REPLACE TABLE {T('gold_daily_revenue')} AS
SELECT order_date, COUNT(*) AS orders, ROUND(SUM(amount), 2) AS revenue
FROM {so}
GROUP BY order_date
''')

spark.sql(f'''
CREATE OR REPLACE TABLE {T('gold_top_products')} AS
SELECT product, COUNT(*) AS orders, ROUND(SUM(amount), 2) AS revenue
FROM {so}
GROUP BY product
''')

spark.sql(f'''
CREATE OR REPLACE TABLE {T('gold_revenue_by_segment')} AS
SELECT COALESCE(c.segment, 'Unknown') AS segment, ROUND(SUM(o.amount), 2) AS revenue
FROM {so} o
LEFT JOIN {sc} c ON o.customer_id = c.customer_id
GROUP BY COALESCE(c.segment, 'Unknown')
''')

display(spark.table(T('gold_top_products')).orderBy(F.desc('revenue')))

# COMMAND ----------

# 1. Membuat tabel gold_revenue_by_city
spark.sql(f'''
CREATE OR REPLACE TABLE {T('gold_revenue_by_city')} AS
SELECT city, COUNT(*) AS orders, ROUND(SUM(amount), 2) AS revenue
FROM {so}
GROUP BY city
''')

# 2. Membuat tabel rata-rata nilai pesanan (Average Order Value) per segmen
spark.sql(f'''
CREATE OR REPLACE TABLE {T('gold_aov_by_segment')} AS
SELECT COALESCE(c.segment, 'Unknown') AS segment,
       COUNT(*) AS total_orders,
       ROUND(AVG(o.amount), 2) AS avg_order_value
FROM {so} o
LEFT JOIN {sc} c ON o.customer_id = c.customer_id
GROUP BY COALESCE(c.segment, 'Unknown')
''')

display(spark.table(T('gold_revenue_by_city')).orderBy(F.desc('revenue')))
display(spark.table(T('gold_aov_by_segment')).orderBy(F.desc('avg_order_value')))