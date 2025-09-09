from pyflink.table import Table
from pyflink.table.expressions import col, year, month, lit, call
from utils.schema_utils import std_regimen

# Maps raw CarePro service names to modules
SERVICE_MAP = {
    # MCH
    "ANC 1st Time On ART": "MCH Module",
    "ANC Delivery & Discharge": "MCH Module",
    "ANC Delivery & Discharge (Baby)": "MCH Module",
    "ANC Follow Up": "MCH Module",
    "ANC Follow Up PMTCT": "MCH Module",
    "ANC Initial Already On ART": "MCH Module",
    "ANC Labour & Delivery": "MCH Module",
    "ANC Labour & Delivery PMTCT": "MCH Module",
    "ANC Labour & Delivery Summary": "MCH Module",
    "ANC Service": "MCH Module",
    "Birth records": "MCH Module",
    "Family Planning": "MCH Module",
    "Partograph": "MCH Module",
    "Postnatal Adult": "MCH Module",
    "Postnatal PMTCT Adult": "MCH Module",
    "Postnatal PMTCT Pediatric": "MCH Module",
    "Postnatal Pediatric": "MCH Module",
    "Under Five": "MCH Module",
    # ART
    "ART": "ART Module",
    "ART Follow Up": "ART Module",
    "ART IHPAI": "ART Module",
    "ART Pediatric": "ART Module",
    "ART Stable On Care": "ART Module",
    "HTS": "ART Module",
    "PEP": "ART Module",
    "Pediatric Follow Up": "ART Module",
    "Pediatric IHPAI": "ART Module",
    "PrEP": "ART Module",
    # TB/Other
    "TB FollowUp": "TB",
    "TB Screening": "TB",
    "Covax": "Other",
    "Covid": "Other",
    "Death records": "Other",
    "Intra Transfusion Vital": "Other",
    "Pre Transfusion Vital": "Other",
    "Adverse Event": "Other",
    "VMMC": "Other",
}

def curate_interactions(t: Table) -> Table:
    # Expect columns: clientid, encounterid, interactiondate, servicename, facilityname, district, province
    return (
        t.add_columns(year(col('interactiondate')).alias('interaction_year'))
         .add_columns(month(col('interactiondate')).alias('interaction_month'))
         .add_columns(lit(0).alias('_tmp'))
         .add_or_replace_columns(col('servicename'))
         .map_columns(lambda r: r.set_field('service_category', lit(SERVICE_MAP.get(r['servicename'], r['servicename']))))
         .drop_columns('_tmp')
    )