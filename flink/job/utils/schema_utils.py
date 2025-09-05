# -*- coding: utf-8 -*-
"""
Schema/utils that mirror Stata data-cleaning semantics:

- Robust date parsing/normalization
- Numeric coercion
- BMI & period calculations
- Regimen normalization and family mapping
- MMD bucketing, DSD model mapping
- AHD, VL eligibility/coverage, outcome_1 classification
- Linkage and ART cohort buckets
"""

import re
from math import ceil
from datetime import datetime, date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Optional


# ---------------------------
# Text helpers
# ---------------------------

_WS = re.compile(r"\s+")

def norm_text(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    return _WS.sub(" ", s)

def upper_strip(x: Any) -> Optional[str]:
    s = norm_text(x)
    return s.upper() if s else None

def lower_strip(x: Any) -> Optional[str]:
    s = norm_text(x)
    return s.lower() if s else None


# ---------------------------
# Dates & numbers
# ---------------------------

_DATE_FORMATS = [
    "%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y",
    "%d-%m-%Y", "%m-%d-%Y", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
]

def to_date(x: Any) -> Optional[date]:
    if x is None or x == "":
        return None
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    if isinstance(x, datetime):
        return x.date()
    s = str(x).strip()
    if not s:
        return None
    # ISO fast-path
    try:
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return datetime.fromisoformat(s[:10]).date()
    except Exception:
        pass
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None

def to_int(x: Any) -> Optional[int]:
    if x is None or x == "":
        return None
    try:
        return int(str(x).strip())
    except Exception:
        return None

def to_decimal(x: Any) -> Optional[Decimal]:
    if x is None or x == "":
        return None
    try:
        return Decimal(str(x).strip())
    except (InvalidOperation, ValueError):
        return None

def days_between(a: Optional[date], b: Optional[date]) -> Optional[int]:
    if not a or not b:
        return None
    return (a - b).days

def months_between(a: Optional[date], b: Optional[date]) -> Optional[int]:
    if not a or not b:
        return None
    return int(Decimal(days_between(a, b)) / Decimal("30.437"))


# ---------------------------
# Anthropometrics
# ---------------------------

def bmi(weight_kg: Any, height_cm: Any) -> Optional[Decimal]:
    w = to_decimal(weight_kg)
    h_cm = to_decimal(height_cm)
    if w is None or h_cm is None or h_cm <= 0:
        return None
    h_m = h_cm / Decimal(100)
    try:
        return (w / (h_m * h_m)).quantize(Decimal("0.01"))
    except Exception:
        return None


# ---------------------------
# Regimen normalization
# ---------------------------

# Regex rules: (pattern, canonical regimen)
_REGIMEN_RULES = [
    # DTG
    (r"(TDF|TENOFOVIR).*(3TC|FTC).*(DTG|DOLUTEGRAVIR)|\bTLD\b", "TDF+3TC+DTG"),
    (r"(AZT|ZDV|ZIDOVUDINE).*(3TC|LAMIVUDINE).*(DTG|DOLUTEGRAVIR)", "AZT+3TC+DTG"),
    (r"(ABC|ABACAVIR).*(3TC|LAMIVUDINE).*(DTG|DOLUTEGRAVIR)", "ABC+3TC+DTG"),
    (r"(TAF|TENOFOVIR ALAFENAMIDE).*(FTC|EMTRICITABINE).*(DTG|DOLUTEGRAVIR)", "TAF+FTC+DTG"),
    # EFV
    (r"(TDF|TENOFOVIR).*(3TC|FTC).*(EFV|EFAVIRENZ)|\bTLE(400|600)?\b", "TDF+3TC+EFV"),
    (r"(AZT|ZDV).*(3TC|LAMIVUDINE).*(EFV|EFAVIRENZ)", "AZT+3TC+EFV"),
    (r"(ABC|ABACAVIR).*(3TC|LAMIVUDINE).*(EFV|EFAVIRENZ)", "ABC+3TC+EFV"),
    # NVP
    (r"(AZT|ZDV).*(3TC|LAMIVUDINE).*(NVP|NEVIRAPINE)", "AZT+3TC+NVP"),
    (r"(ABC|ABACAVIR).*(3TC|LAMIVUDINE).*(NVP|NEVIRAPINE)", "ABC+3TC+NVP"),
    (r"(TDF|TENOFOVIR).*(3TC|FTC).*(NVP|NEVIRAPINE)", "TDF+3TC+NVP"),
    # PI boosted
    (r"(LPV/?R|LOPINAVIR/?RITONAVIR|KALETRA)", "LPV/r+BACKBONE"),
    (r"(ATV/?R|ATAZANAVIR/?RITONAVIR)", "ATV/r+BACKBONE"),
    (r"(DRV/?R|DARUNAVIR/?RITONAVIR)", "DRV/r+BACKBONE"),
    # Legacy
    (r"(D4T|STAVUDINE).*(3TC|LAMIVUDINE).*(EFV|NVP)", "d4T+3TC+NNRTI"),
]
_REGEX = [(re.compile(p, re.I), canon) for p, canon in _REGIMEN_RULES]

def normalize_regimen(raw: Any) -> Optional[str]:
    s = upper_strip(raw)
    if not s:
        return None
    s = s.replace("/", "+").replace(",", "+").replace(" ", "")
    for rx, canon in _REGEX:
        if rx.search(s):
            return canon
    parts = re.split(r"\+", s)
    if 2 <= len(parts) <= 4:
        return "+".join(parts)
    return s

def regimen_family(reg: Optional[str]) -> Optional[str]:
    r = upper_strip(reg)
    if not r:
        return None
    if "DTG" in r:
        return "DTG"
    if "EFV" in r:
        return "EFV"
    if "NVP" in r:
        return "NVP"
    if "LPV/R" in r or "ATV/R" in r or "DRV/R" in r or "BACKBONE" in r:
        return "PI/r"
    return "OTHER"


# ---------------------------
# Stata-style buckets
# ---------------------------

def linkage_bucket(days: Optional[int]) -> Optional[str]:
    if days is None:
        return None
    if days < 0: return "ART_B_4_HIV"
    if days <= 7: return "0-7"
    if days <= 14: return "8-14"
    if days <= 30: return "15-30"
    if days <= 90: return "31-90"
    return "90+"

def cohort_bucket(months_on_art: Optional[int]) -> Optional[str]:
    if months_on_art is None:
        return None
    bounds = [3,6,12,24,36,48,60,72,84,96]
    labels = ["0-3","4-6","7-12","13-24","25-36","37-48","49-60","61-72","73-84","85-96","97+"]
    for i, b in enumerate(bounds):
        if months_on_art <= b:
            return labels[i]
    return labels[-1]

def mmd_type_from_duration(days_or_months: Any) -> Optional[str]:
    """If value looks like days, convert to months. If already months, use ceil."""
    if days_or_months is None or str(days_or_months) == "":
        return None
    d = to_decimal(days_or_months)
    if d is None:
        return None
    # heuristics: values > 15 probably days
    months = Decimal(ceil((d / Decimal(30)).quantize(Decimal("0.01")))) if d > 15 else Decimal(ceil(d))
    m = int(months)
    if m < 3: return "<3M"
    if 3 <= m <= 5: return "3-5M"
    if m == 6: return "6M"
    if 7 <= m <= 12: return "7-12M"
    return "13M+"

def dsd_model_f(dsd_text: Any, mmd_type: Optional[str]) -> Optional[str]:
    s = upper_strip(dsd_text) or ""
    if "COMMUNITY" in s or "CPD" in s or "CAG" in s:
        return "Community"
    if "FAST" in s or "FTF" in s:
        return "Fast Track"
    if "APPOINTMENT" in s or "APPT" in s:
        return "Appointment Spacing"
    if mmd_type and mmd_type not in ("<3M",):
        return "MMD"
    return s or None

def vl_baseline_category(baseline_vl_copies: Any, baseline_vl_date: Optional[date], art_start: Optional[date]) -> Optional[str]:
    """Categorize baseline VL by copies & timing relative to ART start (simple mirror)."""
    vl = to_decimal(baseline_vl_copies)
    if vl is None:
        return None
    # timing
    period = None
    if baseline_vl_date and art_start:
        diff_m = months_between(baseline_vl_date, art_start)
        if diff_m is not None:
            if diff_m <= 6: period = "≤6m"
            elif diff_m <= 12: period = "7-12m"
            else: period = ">12m"
    # load buckets
    if vl < 50: band = "<50"
    elif vl < 200: band = "50-199"
    elif vl < 1000: band = "200-999"
    else: band = "≥1000"
    return f"{band}{' @'+period if period else ''}"

def ahd_flag(whostage_current: Any, current_cd4: Any, age_years: Any) -> Optional[bool]:
    """Advanced HIV Disease: WHO stage 3/4 OR CD4<200 in ≥10y, or age <5y."""
    stage = upper_strip(whostage_current) or ""
    if "4" in stage or "STAGE 4" in stage or "III" in stage or "IV" in stage or "3" in stage:
        return True
    cd4 = to_decimal(current_cd4)
    age = to_int(age_years)
    if age is not None and age < 5:
        return True
    if cd4 is not None and cd4 < 200:
        return True
    return False

def vl_eligibility(on_art: bool, months_on_art: Optional[int], anc_visit_date: Optional[date]) -> bool:
    if on_art and ((months_on_art or 0) >= 3 or anc_visit_date is not None):
        return True
    return False

def vl_coverage(vl_date: Optional[date], ref_date: Optional[date], window_months: int) -> Optional[bool]:
    if not ref_date or not vl_date:
        return None
    months = months_between(ref_date, vl_date)
    if months is None:
        return None
    return months <= window_months

def outcome_1(patientstatus: Optional[str],
              re_initiated: bool,
              days_missed: Optional[int]) -> Optional[str]:
    """
    Simplified mirror of Stata outcome buckets:
    - 'TX_RTT' if re-initiated
    - 'TX_ML' if days_missed >= 90 or status indicates TO/Stopped/Dead
    - 'IIT'   if 28 <= days_missed < 90
    - 'NET_NEW' if newly started within the report window (handled upstream usually)
    - else 'Active'
    """
    s = upper_strip(patientstatus) or ""
    if re_initiated:
        return "TX_RTT"
    if "TRANSFER" in s or "STOP" in s or "DEAD" in s:
        return "TX_ML"
    if days_missed is not None:
        if days_missed >= 90:
            return "TX_ML"
        if days_missed >= 28:
            return "IIT"
    return "Active"
