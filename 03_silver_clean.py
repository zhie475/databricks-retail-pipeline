# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

good, bad = clean_orders(spark.table(T('bronze_orders')))
good.write.mode('overwrite').saveAsTable(T('silver_orders'))
bad.write.mode('overwrite').saveAsTable(T('quarantine_orders'))

(spark.table(T('bronze_customers'))
    .select('customer_id', 'name',
            F.coalesce('segment', F.lit('Unknown')).alias('segment'),
            F.col('address.city').alias('city'))
    .dropDuplicates(['customer_id'])
    .write.mode('overwrite').saveAsTable(T('silver_customers')))

print('bronze:', spark.table(T('bronze_orders')).count())
print('silver:', spark.table(T('silver_orders')).count())
print('quarantine:', spark.table(T('quarantine_orders')).count())
display(spark.table(T('quarantine_orders')).groupBy('_reason').count())