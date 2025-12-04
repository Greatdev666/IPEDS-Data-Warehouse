## 🥈 Silver Layer Documentation — IPEDS Data Pipeline

The Silver Layer is responsible for cleaning, standardizing, casting, and consolidating all raw Bronze IPEDS files into analytically-ready, year-aligned models.
This layer resolves schema differences across years, handles missing fields, normalizes race/ethnicity categories, applies macro-based text cleaning, and unions all years per file type into one final Silver model.

## 📂 Files Processed in the Silver Layer

Below is the list of all IPEDS file families cleaned and standardized:

* ADM — Admissions

* CYYYYA — Completions by award level (gender + race/ethnicity)

* CYYYYB — Completions by race/ethnicity (one record per institution)

* CYYYYC — Completions (race/ethnicity totals + age categories)

* EFFYYYYY — Enrollment (full-year, by race/ethnicity)

* EFYYYYA — Enrollment (fall, by race/ethnicity, gender, attendance status)

* EFYYYYB — Enrollment (fall, by age categories + gender + attendance)

* EFFY_DIST / EF_FDIST — Distance learning (Fall & Full-year)

* FYYYY_F1, FYYYY_F2, FYYYY_F3 — Finance (public, nonprofit, for-profit)

* GRYYYY — Graduation Rate data

* HDYYYY — Institutional characteristics (master dataset)

#### Each one is cleaned individually, then UNION ALL across years to produce a combined Silver model.

## 🧼 1. Admissions (ADM) — 2014 to 2023

* IPEDS does not report ADM prior to 2014, so Silver includes 2014–2023 only.

* Some fields did not exist in earlier years (2014–2021).

* Major structural change in 2022 & 2023:

* New gender categories → Another gender and Unknown gender

* Present in totals and also in full-time / part-time groups:

``` sql 
CAST("ENRLAN" AS INTEGER) AS "ENRLAN",
CAST("ENRLUN" AS INTEGER) AS "ENRLUN",
CAST("ENRLFTAN" AS INTEGER) AS "ENRLFTAN",
CAST("ENRLFTUN" AS INTEGER) AS "ENRLFTUN",
```

* Missing fields in older years are cast as NULL::INT to ensure schema alignment before UNION.

## 🧼 2. Completions — CYYYYA, CYYYYB, CYYYYC
### 🅰️ CYYYYA — Completion by Award Level (Gender + Race/Ethnicity)

* Multiple rows per institution (because data is split by award levels).

* Contains gender + race/ethnicity combinations.

### 🅱️ CYYYYB — Completion by Race/Ethnicity (Single Row Per Institution)

* One row per institution.

* Includes totals per race/ethnicity (no award breakdown).

### 🅾️ CYYYYC — Completion by Race/Ethnicity + Age Groups

* Contains race/ethnicity totals

* Includes age categories

### 🔄 Major Race/Ethnicity Standardization (2000–2007 vs 2008+)

IPEDS changed race categories in 2008.

### Before 2008

Asian and Pacific Islander were combined.

### After 2008

They become two separate categories:

* Asian

* Native Hawaiian / Pacific Islander (NHPI)

To standardize:
```sql
CAST("CRACE22" AS INTEGER) AS "CWHITT",
CAST("CRACE11" AS INTEGER) AS "CWHITM",
CAST("CRACE12" AS INTEGER) AS "CWHITW",
```
#### Old fields were transformed to match the modern structure so all years align.

## 🧼 3. Enrollment Files (EF & EFF)
### 📌 Full-Year Enrollment (EFFYYYYY) — Starts 2002

* Broken down by race/ethnicity

* Requires the same pre/post-2008 race mapping as completions

### 📌 Fall Enrollment — EFYYYYA

* Contains:

* race/ethnicity

* gender

* full-time / part-time attendance

* Multiple rows per institution

### 📌 Fall Enrollment — EFYYYYB

* Enrollment by:

* Age categories

* Gender

* Attendance

* Level of student


## 🧼 4. Distance Learning (EFDIST & EFFDIST)

### Two files:

* Fall Distance Learning (EFDIST) — starts 2012

* Full-Year Distance Learning (EFFDIST) — starts 2020

* Standardized by casting all numeric fields and aligning schemas across years.

## 🧼 5. Finance (F1, F2, F3)

#### Three distinct structures:

|  File             | Sector           | Meaning                                    |
|-----------------------|---------------|------------------------------------------------|
| F1                | Public           | Governmental accounting                         |
| F2                  | Nonprofit           | Private not-for-profit                                  |
| F3               | For-profit | Proprietary institutions                         |

### Why Columns Are Cast to NUMERIC

* Avoids integer overflow (BIGINT too risky)

* FLOAT introduces rounding/precision problems

* NUMERIC preserves exact financial values with no approximation

#### Therefore:
```sql 
CAST("F1A01" AS NUMERIC) AS "F1A01"
```

## 🧼 6. Graduation Rates (GRYYYY)

* Cleaned and casted consistently

* Handles missing fields in earlier years

* Unioned into one final Silver model

## 🧼 7. Institutional Characteristics (HDYYYY)

* The master descriptor table

* Contains names, addresses, geography, institution level, control, religion, etc.

* Some fields missing in early years (FAIDURL, INSTSIZE, HDEGOFR1, etc.)

### 🗺 Cleaning via Macros

* We used text-standardization macros:

* clean_text() — For names, cities, aliases
```sql
UPPER(TRIM(REGEXP_REPLACE(col, '\s+', ' ')))
```
#### clean_links() — For URLs
```sql
LOWER(TRIM(REGEXP_REPLACE(col, '\s+', ' ')))
```

### 🏗 Structure

* Each year (2000–2023) has its own cleaned HD model

* A final hdyyyy model UNIONs all years

* Missing fields are filled with NULL + casted correctly

* HD becomes the foundation for dimension tables in Gold


## 🔗 8. UNION ALL Strategy

* After cleaning each dataset:

* Select the final list of aligned columns

* Cast all to consistent types

* Apply cleaning macros

* Union all years:
```sql
SELECT * FROM {{ ref('hd2023') }}
UNION ALL
SELECT * FROM {{ ref('hd2022') }}
...
UNION ALL
SELECT * FROM {{ ref('hd2000') }}
```

### Why Not Incremental?

#### Because each Silver table is:

* fully cleaned

* not time-consuming to rebuild

* must preserve historical corrections (re-running improves quality)

## Silver focuses on:

* Standardization

* Data type casting

* Schema alignment

* Cleaning (text + URLs)

* Race/ethnicity mapping

* Null handling


## ✔️ 10. What We Achieved in Silver Layer (Summary)

* Cleaned ALL IPEDS raw files from Bronze

* Standardized schemas across 500+ variations

* Handled years with missing/extra columns

* Applied consistent text/url cleaning

* Normalized race/ethnicity across 2000–2023

* Casted all fields to safe and analysis-ready types

* Built UNION ALL models for each dataset

* Prepared stable inputs for Gold Layer fact/dimension modeling

* Ensured the Bronze layer remains available for future deep-dive needs

## 🚀 11. Silver Layer is Complete — Next Step: Gold Layer

* In the Gold Layer, we will:

* Build fact tables (Admissions, Enrollment, Completion, Finance, Graduation Rates)

* Build dimension tables (Institutions, Geography, Race/Ethnicity, etc.)

* Apply metrics, KPIs, aggregations

* Fully document models

* Add tests (unique keys, relationships, accepted values)