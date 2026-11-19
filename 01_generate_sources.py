# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

import pandas as pd, numpy as np, json, os

rng = np.random.default_rng(42)
n = 200_000
orders = pd.DataFrame({
    'order_id': rng.integers(1, 190_000, n),
    'customer_id': rng.integers(1, 21_000, n),
    'product': rng.choice(['laptop', 'Phone', 'MOUSE', ' keyboard', 'monitor', 'cable'], n),
    'city': rng.choice(['Jakarta', 'Pekanbaru', 'Medan', 'Bandung', None], n),
    'amount': rng.normal(80, 60, n).round(2),
    'order_date': (pd.to_datetime('2026-01-01')
                   + pd.to_timedelta(rng.integers(0, 90, n), unit='D')).strftime('%Y-%m-%d')
})
bad = orders.sample(frac=0.02, random_state=1).index
orders.loc[bad, 'order_date'] = pd.to_datetime(orders.loc[bad, 'order_date']).dt.strftime('%d/%m/%Y')

os.makedirs(f'{BASE}/orders', exist_ok=True)
orders.to_csv(f'{BASE}/orders/orders_2026q1.csv', index=False)

os.makedirs(f'{BASE}/customers', exist_ok=True)
with open(f'{BASE}/customers/customers.json', 'w') as f:
    for c in range(1, 20_001):
        seg = str(rng.choice(['Retail', 'Business', 'Student', '']))
        rec = {
            'customer_id': c,
            'name': f'Customer {c}',
            'segment': seg if seg else None,
            'address': {
                'city': str(rng.choice(['Jakarta', 'Pekanbaru', 'Medan', 'Bandung'])),
                'country': 'ID'
            }
        }
        f.write(json.dumps(rec) + '\n')

print(dbutils.fs.ls(BASE))