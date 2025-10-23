import csv
import os
from datetime import datetime, timedelta
import random

# Output local path matching dataset date structure
base_dir = os.path.join('data', 'raw', 'payments', '2025', '09', '26')
os.makedirs(base_dir, exist_ok=True)
outfile = os.path.join(base_dir, 'sample.csv')

# Columns expected by payments_v1 standardization rules
fieldnames = [
    'payment_id',      # already in target format
    'customerId',      # will be renamed to customer_id
    'amount',
    'currency',
    'payment_date',
    'updated_at',
    'IVA'              # will be renamed to iva
]

random.seed(42)
start_dt = datetime(2025, 9, 20)

rows = []
for i in range(200):
    dt = start_dt + timedelta(days=random.randint(0, 10), hours=random.randint(0, 23), minutes=random.randint(0, 59))
    upd = dt + timedelta(minutes=random.randint(0, 120))
    rows.append({
        'payment_id': f'P{i+1:06d}',
        'customerId': f'C{i%50+1:05d}',
        'amount': round(random.uniform(10, 5000), 2),
        'currency': 'CLP',
        'payment_date': dt.strftime('%Y-%m-%d %H:%M:%S'),
        'updated_at': upd.strftime('%Y-%m-%d %H:%M:%S'),
        'IVA': round(random.uniform(0, 100), 2),
    })

with open(outfile, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"Sample CSV written: {outfile} ({len(rows)} rows)")