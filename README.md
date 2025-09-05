Notes & what this gives you

Exact column parity with your source list in the destination carepro.art_cohort.

Regimen cleaning uses a comprehensive regex catalogue that covers DTG/EFV/NVP/PIs and common nicknames (TLD/TLE etc.).

Date “formatting” in Stata is implemented as date normalization (into proper DATE types). Superset can format for display; you retain correct types for analytics.

BMI and vitals are computed if missing (from weight/height).

Stateful dedup prevents noisy re-writes (hash per (patientid,artnumber)).

How this lines up with your Stata

Regimen cleaning: canonical DTG/EFV/NVP/PI/r (inc. TLD/TLE nicknames) → currentartregimen and family in curr_regimen_based.

On ART if any of ART number / ART start / current regimen present.

Linkage (linkage_period_days, _mo, linkagetocare_cat) and ART cohort (artclient_cohortmo, art_cohort) follow your thresholds.

VL baseline category combines copies & timing vs ART start.

MMD buckets from dispensationduration; DSD model mapped from dsd text + MMD.

AHD if WHO3/4 or CD4<200 (≥10y) or age<5.

VL eligibility (on ART & ≥3 months or ANC) and coverage within 12m (and relaxed 48m).

Outcome_1 buckets Active / TX_RTT / IIT / TX_ML (with re-initiation & missed-days logic).

Silent transfer and re-initiated flags baked in.