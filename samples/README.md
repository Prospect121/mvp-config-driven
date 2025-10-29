# Toy datasets

El directorio contiene datos sintéticos utilizados en las pruebas de humo multi-nube.

- `toy_customers.csv`: subconjunto de clientes con columnas `CUSTOMER_ID`, `SEGMENT`, `STATUS` y `SIGNUP_TS`.

Puedes regenerar el archivo en caliente ejecutando:

```bash
python - <<'PY'
from datetime import datetime, timedelta
import csv

rows = [
    ("C-0001", "enterprise", "active", "2024-01-05T10:31:00Z"),
    ("C-0002", "consumer", "inactive", "2024-01-12T16:05:00Z"),
    ("C-0003", "startup", "active", "2024-01-20T08:12:00Z"),
    ("C-0004", "consumer", "active", "2024-02-02T13:45:00Z"),
    ("C-0005", "enterprise", "inactive", "2024-02-15T09:10:00Z"),
]

with open("samples/toy_customers.csv", "w", newline="", encoding="utf-8") as handle:
    writer = csv.writer(handle)
    writer.writerow(["CUSTOMER_ID", "SEGMENT", "STATUS", "SIGNUP_TS"])
    writer.writerows(rows)
PY
```

El dataset se integrará en los planes generados por `prodi plan/run`; mientras tanto, los `cfg/<layer>/example.yml` lo referencian en modo `dry_run` para pruebas manuales controladas.
