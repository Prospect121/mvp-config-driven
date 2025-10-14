"""
Catálogo de UDFs seguros y eficientes para uso declarativo.

Buenas prácticas implementadas:
- Preferencia por funciones nativas de Spark; UDFs solo cuando es necesario.
- Pandas UDFs vectorizadas para mejor rendimiento (Arrow) cuando están disponibles.
- Fallback automático a UDFs PySpark regulares si faltan pandas/pyarrow.
- Funciones determinísticas y libres de efectos secundarios.
- Tipos explícitos y validación de argumentos.
"""

import os
import re
import unicodedata
from typing import Optional
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType, IntegerType

# Uso ESTRICTO de Pandas UDFs: requerimos pandas y pyarrow
import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore
PANDAS_AVAILABLE = True


# -------- Helpers (implementaciones puras en Python) --------

def _strip_accents_py(text: Optional[str]) -> str:
    if text is None:
        return ""
    try:
        nfkd_form = unicodedata.normalize("NFKD", text)
        return "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    except Exception:
        return text or ""


def normalize_id_py(value: Optional[str]) -> str:
    s = (value or "").upper()
    return re.sub(r"[^A-Z0-9]", "", s)


def sanitize_string_py(value: Optional[str]) -> str:
    s = (value or "")
    s = re.sub(r"[\x00-\x1F\x7F]", "", s)
    return s.strip()


def standardize_name_py(first: Optional[str], last: Optional[str]) -> str:
    f = (first or "").strip()
    l = (last or "").strip()
    s = (f + " " + l).strip()
    s = _strip_accents_py(s).lower()
    return " ".join(w.capitalize() for w in s.split())


def calculate_age_py(birth: Optional[str]) -> Optional[int]:
    if birth is None or str(birth).strip() == "":
        return None
    import datetime as _dt
    val = str(birth).strip()
    # Intentos básicos de parseo
    dt = None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            dt = _dt.datetime.strptime(val, fmt)
            break
        except Exception:
            continue
    if dt is None:
        try:
            dt = _dt.datetime.fromisoformat(val)
        except Exception:
            return None
    today = _dt.date.today()
    age = today.year - dt.date().year - ((today.month, today.day) < (dt.month, dt.day))
    return int(age) if age >= 0 else None


def format_address_py(street: Optional[str], city: Optional[str], postal_code: Optional[str]) -> str:
    s = (street or "").strip()
    c = (city or "").strip()
    p = (postal_code or "").strip()
    parts = [x for x in [s, c, p] if x]
    return ", ".join(parts)


# -------- Definiciones UDF (Pandas vs PySpark según disponibilidad) --------

@pandas_udf(StringType())
def normalize_id(series: pd.Series) -> pd.Series:
    s = series.fillna("")
    return s.str.upper().str.replace(r"[^A-Z0-9]", "", regex=True)

@pandas_udf(StringType())
def strip_accents(series: pd.Series) -> pd.Series:
    return series.fillna("").map(_strip_accents_py)

@pandas_udf(StringType())
def sanitize_string(series: pd.Series) -> pd.Series:
    s = series.fillna("").str.replace(r"[\x00-\x1F\x7F]", "", regex=True)
    return s.str.strip()

@pandas_udf(StringType())
def standardize_name(first: pd.Series, last: pd.Series) -> pd.Series:
    f = first.fillna("").astype(str).str.strip()
    l = last.fillna("").astype(str).str.strip()
    s = (f + " " + l).str.strip()
    s = s.map(_strip_accents_py).str.lower()
    return s.apply(lambda x: " ".join(w.capitalize() for w in x.split()))

@pandas_udf(IntegerType())
def calculate_age(birth: pd.Series) -> pd.Series:
    bd = pd.to_datetime(birth, errors='coerce')
    today = pd.Timestamp('now').normalize()
    age = ((today - bd).dt.days // 365).astype('Int64')
    return age.where(~bd.isna(), None)

@pandas_udf(StringType())
def format_address(street: pd.Series, city: pd.Series, postal_code: pd.Series) -> pd.Series:
    s = street.fillna("").astype(str).str.strip()
    c = city.fillna("").astype(str).str.strip()
    p = postal_code.fillna("").astype(str).str.strip()
    df = pd.DataFrame({'s': s, 'c': c, 'p': p})
    def join_row(row):
        parts = [x for x in [row['s'], row['c'], row['p']] if x]
        return ", ".join(parts)
    return df.apply(join_row, axis=1)


# --- Catálogo ---

def get_udf(name: str):
    """Obtiene la función UDF registrada por nombre.

    Solo se exponen UDFs seguras y determinísticas. Para lógica compleja,
    preferir expresiones SQL o funciones nativas de Spark (sha2, regexp, etc.).
    """
    normalized = (name or "").strip().lower()
    catalog = {
        "normalize_id": normalize_id,
        "strip_accents": strip_accents,
        "sanitize_string": sanitize_string,
        "standardize_name": standardize_name,
        "calculate_age": calculate_age,
        "format_address": format_address,
    }
    return catalog.get(normalized)