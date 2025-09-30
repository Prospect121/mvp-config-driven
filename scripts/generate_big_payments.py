#!/usr/bin/env python3
import csv, random, datetime

# Configuración
ROWS = 5_000_000   # número de filas a generar (ajusta según RAM/tiempo)
OUT_FILE = "data/raw/payments/big_sample.csv"

# Valores simulados
currencies = ["CLP", "USD", "COP", "MXN", "BRL"]
customers = [f"c-{i}" for i in range(1, 10001)]

# Generador de pagos
def random_payment(i):
    return {
        "payment_id": f"p-{i}",
        "customer_id": random.choice(customers),
        "amount": round(random.uniform(1, 5000), 2),
        "currency": random.choice(currencies),
        "payment_date": (
            datetime.datetime(2020, 1, 1) +
            datetime.timedelta(days=random.randint(0, 1800),
                               seconds=random.randint(0, 86400))
        ).strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

# Escritura
with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["payment_id","customer_id","amount","currency","payment_date","updated_at"])
    writer.writeheader()
    for i in range(1, ROWS + 1):
        writer.writerow(random_payment(i))
        if i % 100000 == 0:
            print(f"{i:,} rows written...")

print(f"Archivo grande generado: {OUT_FILE} con {ROWS:,} filas")
