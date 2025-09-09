# -*- coding: utf-8 -*-
"""
Transform one art_cohort row (JSON) to the cleaned + enriched record:
- Keeps ALL source columns exactly as specified
- Adds Stata-style derived columns:
  on_art, curr_regimen_based, linkage_period_days, linkage_period_mo, linkagetocare_cat,
  artclient_cohortmo, art_cohort, vl_baseline_cat, mmd_type, dsd_model_f,
  ahd, vl_eligibility, vl_coverage_12m, vl_coverage_48m, outcome_1, silent_transfer, re_initiated
"""

from datetime import date
from typing import Dict, Any, Optional
from utils.schema_utils import (
    to_date, to_int, to_decimal, bmi, months_between, days_between,
    normalize_regimen, regimen_family, linkage_bucket, cohort_bucket,
    mmd_type_from_duration, dsd_model_f as dsd_model_calc, vl_baseline_category,
    ahd_flag, vl_eligibility as vl_elig_calc, vl_coverage as vl_cov_calc,
    norm_text, upper_strip, lower_strip
)

# --- Base columns (your source list; DO NOT change) ---
BASE_COLS = [
    "patientid","artnumber","nupn","nrc","owningguid","biometrics","firstname","surname","dob","age",
    "maritalstatus","sex","occupation","educational_level","facilityname","facilityid","facilitymastercode",
    "currentpatientweight","baselinepatientweight","currentpatientheight","baselinepatientheight",
    "district","province","hivtestdate","hivtestresults","whostagingbaseline","whostagingbaselinedate",
    "whostagingcurrent","whostagingcurrentdate","artstartdate","artregimenbaseline","currentartregimen",
    "dispensationduration","currentartregimendispensationdate","baseline_cd4","baselinecd4countdate",
    "current_cd4","currentcd4countdate","baseline_cd4_perc","baselinecd4percdate","current_cd4_perc",
    "currentcd4percdate","baseline_vl_copies","baselinevldate","current_vl_copies","currentvldate",
    "patientstatus","patientstatusdate","ancvisitdate","estimateddateofdelivery","lastinteractionservice",
    "firstinteractionservice","firstinteraction_date","lastinteraction_date","nextappointmentdate","prescription",
    "prescription_date","dsd","entrypoint","discordantcouple","multipleconcurrentpartners","previousfacilityname",
    "transferindate","codeofpreviousfacility","facilitynametransferredto","codeoffacilitytransferredto",
    "proteinuria","proteinuriadate","rprrst","rprrsdate","hb","hbdate","alt","altdate","rbs","rbsdate",
    "creatinine","creatininedate","hepatitisb","hepatitisbdate","meningitis","meningitisdate","csf","csfdate",
    "malaria","malariadate","geneexpertmtb","geneexpertmtbdate","syphilisrdt","syphilisrdtdate","tpha","tphardtdate",
    "hpv","hpvdate","tptplan","onantitb","currentlyhavetb","kindoftb","wasattcompleted","attnotcompletedreason",
    "isontpt","tptstartdate","tptenddate","istakentptthreeyears","ispatienteligible","reasonnotstarted",
    "dateeligiblefortpt","treatmentoutcome","currentbmi","baselinebmi","currentmuac","baselinemuac",
    "currentsystolic","baselinesystolic","currentdiastolic","currentdiastolic_2","currenttemperature",
    "baselinetemperature","currentpulse","baselinepulse","currentheadcircumference","baselineheadcircumference",
    "current_vitalsdate","baseline_vitalsdate","dateofdeath","placeofdeath","causeofdeath",
    "causeofdeathanatomicaxisid","causeofdeathicpc2id","causeofdeathpathologyaxisid"
]

# --- Extra Stata-style derived columns we add ---
EXTRA_COLS = [
    "on_art","curr_regimen_based",
    "linkage_period_days","linkage_period_mo","linkagetocare_cat",
    "artclient_cohortmo","art_cohort",
    "vl_baseline_cat","mmd_type","dsd_model_f",
    "ahd","vl_eligibility","vl_coverage_12m","vl_coverage_48m",
    "outcome_1","silent_transfer","re_initiated"
]

REQUIRED_COLS = BASE_COLS + EXTRA_COLS

DATE_FIELDS = {
    "dob","hivtestdate","whostagingbaselinedate","whostagingcurrentdate","artstartdate",
    "currentartregimendispensationdate","baselinecd4countdate","currentcd4countdate","baselinecd4percdate",
    "currentcd4percdate","baselinevldate","currentvldate","patientstatusdate","ancvisitdate",
    "estimateddateofdelivery","firstinteraction_date","lastinteraction_date","nextappointmentdate",
    "prescription_date","proteinuriadate","rprrsdate","hbdate","altdate","rbsdate","creatininedate",
    "hepatitisbdate","meningitisdate","csfdate","malariadate","geneexpertmtbdate","syphilisrdtdate",
    "tphardtdate","hpvdate","tptstartdate","tptenddate","dateeligiblefortpt","current_vitalsdate","baseline_vitalsdate",
    "dateofdeath","transferindate"
}

NUMERIC_FIELDS = {
    "currentpatientweight","baselinepatientweight","currentpatientheight","baselinepatientheight",
    "baseline_cd4","current_cd4","baseline_cd4_perc","current_cd4_perc",
    "baseline_vl_copies","current_vl_copies","hb","alt","rbs","creatinine",
    "currentsystolic","baselinesystolic","currentdiastolic","currentdiastolic_2",
    "currenttemperature","baselinetemperature","currentpulse","baselinepulse",
    "currentheadcircumference","baselineheadcircumference",
    "dispensationduration","currentbmi","baselinebmi","currentmuac","baselinemuac"
}

def _get(d: Dict[str, Any], *names: str) -> Any:
    for n in names:
        if n in d and d[n] not in (None, ""):
            return d[n]
    return None

def _ensure_all(out: Dict[str, Any]):
    for c in REQUIRED_COLS:
        out.setdefault(c, None)

def transform(row: Dict[str, Any]) -> Dict[str, Any]:
    # Canonicalize keys to lowercase-ish for lookup
    r = { (k.lower() if isinstance(k, str) else k): v for k, v in row.items() }

    out: Dict[str, Any] = {}

    # --- text passthroughs ---
    passthru = set(BASE_COLS) - DATE_FIELDS - NUMERIC_FIELDS - {"age"}
    for k in passthru:
        out[k] = norm_text(_get(r, k.lower(), k.replace("_", "")))

    # --- dates ---
    for k in DATE_FIELDS:
        out[k] = to_date(_get(r, k.lower(), k.replace("_", "")))

    # --- numerics ---
    for k in NUMERIC_FIELDS:
        out[k] = to_decimal(_get(r, k.lower(), k.replace("_", "")))

    # --- age (integer) ---
    out["age"] = to_int(_get(r, "age"))

    # --- regimen cleaning (baseline & current) ---
    base_reg = _get(r, "artregimenbaseline", "baselineartregimen")
    curr_reg = _get(r, "currentartregimen", "currentregimen")
    out["artregimenbaseline"] = normalize_regimen(base_reg)
    out["currentartregimen"] = normalize_regimen(curr_reg)
    out["curr_regimen_based"] = regimen_family(out["currentartregimen"])

    # --- on_art flag ---
    out["on_art"] = bool(out.get("artnumber") or out.get("artstartdate") or out.get("currentartregimen"))

    # --- BMI fallbacks if missing ---
    if out.get("currentbmi") is None:
        out["currentbmi"] = bmi(out.get("currentpatientweight"), out.get("currentpatientheight"))
    if out.get("baselinebmi") is None:
        out["baselinebmi"] = bmi(out.get("baselinepatientweight"), out.get("baselinepatientheight"))

    # --- canonical owningguid lower-case ---
    if out.get("owningguid"):
        og = str(out["owningguid"]).strip()
        out["owningguid"] = og.lower()

    # --- linkage & cohorts ---
    hiv_dt = out.get("hivtestdate")
    art_start = out.get("artstartdate")
    report_ref = out.get("lastinteraction_date") or out.get("current_vitalsdate") or out.get("patientstatusdate") or date.today()
    link_days = days_between(art_start, hiv_dt)
    out["linkage_period_days"] = link_days
    out["linkage_period_mo"] = months_between(art_start, hiv_dt)
    out["linkagetocare_cat"] = linkage_bucket(link_days)
    mo_on_art = months_between(report_ref, art_start)
    out["artclient_cohortmo"] = mo_on_art
    out["art_cohort"] = cohort_bucket(mo_on_art)

    # --- VL baseline category ---
    out["vl_baseline_cat"] = vl_baseline_category(out.get("baseline_vl_copies"), out.get("baselinevldate"), art_start)

    # --- MMD & DSD ---
    out["mmd_type"] = mmd_type_from_duration(out.get("dispensationduration"))
    out["dsd_model_f"] = dsd_model_calc(out.get("dsd"), out.get("mmd_type"))

    # --- Silent transfer & re-initiated (approx mirror) ---
    prev_fac = (out.get("previousfacilityname") or "").strip().lower() if out.get("previousfacilityname") else ""
    curr_fac = (out.get("facilityname") or "").strip().lower() if out.get("facilityname") else ""
    out["silent_transfer"] = bool(prev_fac and curr_fac and prev_fac != curr_fac and (upper_strip(out.get("patientstatus") or "") != "TRANSFER OUT"))
    out["re_initiated"] = bool(out.get("lastinteraction_date") and out.get("artstartdate") and out["lastinteraction_date"] == out["artstartdate"] and out["silent_transfer"])

    # --- AHD flag ---
    out["ahd"] = ahd_flag(out.get("whostagingcurrent"), out.get("current_cd4"), out.get("age"))

    # --- VL eligibility & coverage ---
    out["vl_eligibility"] = vl_elig_calc(out["on_art"], mo_on_art, out.get("ancvisitdate"))
    out["vl_coverage_12m"] = vl_cov_calc(out.get("currentvldate"), report_ref, 12)
    out["vl_coverage_48m"] = vl_cov_calc(out.get("currentvldate"), report_ref, 48)

    # --- outcome_1 bucket (Active / TX_RTT / IIT / TX_ML / NET_NEW*) ---
    # Days missed = days since next appointment (if available), else since last interaction
    days_missed = None
    if out.get("nextappointmentdate"):
        days_missed = days_between(report_ref, out["nextappointmentdate"])
    elif out.get("lastinteraction_date"):
        days_missed = days_between(report_ref, out["lastinteraction_date"])
    out["outcome_1"] = __outcome_bucket(out.get("patientstatus"), out["re_initiated"], days_missed)

    # Ensure ALL columns exist
    _ensure_all(out)
    return out


def __outcome_bucket(patientstatus, re_initiated, days_missed) -> str:
    from .utils.schema_utils import outcome_1 as _o1
    return _o1(patientstatus, re_initiated, days_missed)
