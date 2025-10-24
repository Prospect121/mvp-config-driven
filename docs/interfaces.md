# Contrato de interfaces — DataFrame in / DataFrame out

Propósito
--------
Definir el contrato mínimo que deben cumplir las funciones de transformación dentro del core `datacore` y los adaptadores por capa. Esto facilita mover transformaciones existentes al nuevo scaffolding sin cambiar semántica.

Contrato
--------
- Entrada: un DataFrame en memoria o distribuido (Spark DataFrame, o polars/pandas DataFrame para tests locales).
- Salida: un DataFrame con esquema definido (columnas y tipos) listo para persistir.
- Forma de invocación: func(df: DataFrame, cfg: dict) -> DataFrame

Reglas y convenciones
---------------------
- Todas las transformaciones deben recibir y devolver DataFrames; evitar accesos side-effect directos a sinks dentro de la transformación.
- La variable placeholder SQL para plantillas será `__INPUT__` (ej.: `SELECT * FROM __INPUT__ WHERE x IS NOT NULL`).
- La responsabilidad de persistir se delega a `datacore.io` (fs wrappers) o a `layers/*/main.py`.
- Las UDFs deben registrarse mediante helpers en `datacore.transforms` para evitar acoplamientos al motor.

Tipos aceptados (implementación mínima)
--------------------------------------
- Spark DataFrame (preferido en prod): `pyspark.sql.DataFrame`.
- Polars DataFrame (tests/local): `polars.DataFrame`.
- Pandas DataFrame (muy pequeños tests): `pandas.DataFrame`.

Adaptadores
----------
El core debe exponer adaptadores que conviertan entre estos tipos cuando sea necesario (por ejemplo, para ejecutar SQL en Spark o para ejecutar transformaciones en polars durante tests rápidos).

Ejemplo de uso (pseudocódigo)
----------------------------
```py
# carga
df = fs.read_df(source_cfg)
# transformación
df2 = transforms.apply_sql(df, cfg['transform'])
# validación
dq.run_expectations(df2, cfg.get('dq'))
# persistir
fs.write_df(df2, sink_cfg)
```

Notas
-----
- Mantener compatibilidad con las transformaciones existentes: si una función actual no cumple el contrato, añadir un adaptador que la envuelva.
- Evitar cambiar nombres de columnas ni particionado por defecto salvo que un test snapshot detecte un bug y lo corrija explícitamente.
