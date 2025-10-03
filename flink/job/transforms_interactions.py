# -*- coding: utf-8 -*-
"""
Transform/curation for carepro_all_interactions
- Adds: service_category (UDF), interaction_year (UDF), interaction_month (UDF)
- Version-safe across PyFlink builds (no YEAR/MONTH/CASE/IF SQL calls)
- Self-contained: no external utils
"""

import re
from datetime import datetime
from typing import Optional

from pyflink.table import Table, DataTypes
from pyflink.table.expressions import col, lit, call
from pyflink.table.udf import udf


# -----------------------------
# Helpers (Python-side)
# -----------------------------

_DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%m-%d-%Y",
]

def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None
    # ISO fast-path
    try:
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            # Accept YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS(.fff)
            try:
                return datetime.fromisoformat(s[:19])  # up to seconds if present
            except ValueError:
                try:
                    return datetime.fromisoformat(s[:10])  # date only
                except ValueError:
                    pass
    except Exception:
        pass
    # Try known formats
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    # Last-resort regex: first YYYY-MM or YYYY/MM
    m = re.match(r"^\s*(\d{4})[-/](\d{1,2})", s)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        try:
            return datetime(y, mo, 1)
        except Exception:
            return None
    # Plain year at start
    m = re.match(r"^\s*(\d{4})\b", s)
    if m:
        try:
            return datetime(int(m.group(1)), 1, 1)
        except Exception:
            return None
    return None


# -----------------------------
# UDFs
# -----------------------------

@udf(result_type=DataTypes.STRING())
def service_category_udf(servicename: str) -> str:
    """
    Priority buckets:
      ART -> MCH -> TB -> LAB -> PHARMACY -> OTHER
    """
    if not servicename:
        return "OTHER"
    s = str(servicename).upper()

    # ART/HIV family
    if any(k in s for k in (" ART", "ART ", "HIV", " VL", "VL ", "HTS", "PREP", " PEP", "PEP ")):
        return "ART Module"

    # MCH family
    if ("ANC" in s) or ("POSTNATAL" in s) or ("UNDER FIVE" in s) or ("FAMILY PLANNING" in s) or ("MCH" in s):
        return "MCH Module"

    # TB
    if "TB" in s:
        return "TB"

    # LAB / PHARMACY
    if "LAB" in s:
        return "LAB"
    if "PHARM" in s:
        return "PHARMACY"

    return "OTHER"


@udf(result_type=DataTypes.STRING())
def year_from_str_udf(dt_str: str) -> str:
    dt = _parse_dt(dt_str)
    return f"{dt.year:04d}" if dt else None


@udf(result_type=DataTypes.STRING())
def month_from_str_udf(dt_str: str) -> str:
    dt = _parse_dt(dt_str)
    return f"{dt.month:02d}" if dt else None


# -----------------------------
# Main curation entrypoint
# -----------------------------

def curate_interactions(t: Table) -> Table:
    """
    Adds columns:
      - service_category (STRING via UDF)
      - interaction_year (STRING via UDF)
      - interaction_month (STRING via UDF)
    Leaves original columns unchanged.
    """
    return (
        t
        .add_columns(service_category_udf(col('servicename')).alias('service_category'))
        .add_columns(year_from_str_udf(col('interactiondate')).alias('interaction_year'))
        .add_columns(month_from_str_udf(col('interactiondate')).alias('interaction_month'))
    )
