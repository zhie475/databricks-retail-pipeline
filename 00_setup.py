# Databricks notebook source
from pyspark.sql import functions as F
CATALOG = 'workspace'
SCHEMA = 'pipeline_demo'
VOLUME = 'landing'
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}')
spark.sql(f'CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}')
BASE = f'/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}'

def T(name):
    return f'{CATALOG}.{SCHEMA}.{name}'

print("Schema:", f"{CATALOG}.{SCHEMA}")
print("Landing path:", BASE)

def clean_orders(df):
    p = (df.withColumn('product', F.initcap(F.trim('product')))
           .withColumn('order_date', F.coalesce(F.try_to_date('order_date', 'yyyy-MM-dd'),
                                                F.try_to_date('order_date', 'dd/MM/yyyy')))
           .withColumn('city', F.coalesce('city', F.lit('Unknown'))))
    valid = F.coalesce((F.col('order_id').isNotNull()) & (F.col('amount') > 0)
                       & F.col('order_date').isNotNull(), F.lit(False))
    reason = (F.when(F.col('order_id').isNull(), 'missing order_id')
               .when(F.col('amount').isNull() | (F.col('amount') <= 0), 'invalid amount')
               .otherwise('unparseable date'))
    good = p.filter(valid).dropDuplicates(['order_id'])
    bad = p.filter(~valid).withColumn('_reason', reason)
    return good, bad