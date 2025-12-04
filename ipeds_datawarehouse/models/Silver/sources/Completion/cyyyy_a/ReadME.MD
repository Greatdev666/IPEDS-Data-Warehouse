# 📄 CYYYY_A — README

## 📌 Overview

The `cyyyy_a` model standardizes and unifies IPEDS Completions (C-Series) – Race/Ethnicity data across multiple years (2000–present).

Because IPEDS changed the race/ethnicity structure multiple times, this model contains a series of cleaning, harmonization, and alignment rules to produce a single consistent dataset.

---

## 📂 Source Tables Included

| Year Range      | Raw Table Pattern     | Notes                                                                 |
|-----------------|--------------------|----------------------------------------------------------------------|
| 2000–2007       | Bronze.cYYYY_a      | Uses old race categories (e.g., CRACE20, CRACE07, CRACE08). No separate Asian vs NHPI. |
| 2008–2010       | Bronze.cYYYY_a      | Mixed reporting: some institutions used old race codes, some used new. |
| 2011–present    | Bronze.cYYYY_a      | Uses modern race structure (Asian, NH/PI separate).                  |

All tables are unioned after harmonization.

---

## ⚠️ Data Issues Identified

### 1. Mixed Race Coding (2008–2010)

Institutions inconsistently applied the transition from old IPEDS race categories to the new standard.

**Problems observed:**
- Some rows use old categories (`CRACE20`, `CRACE07`, `CRACE08`).
- Some rows use new categories (`CNHPIT`, `CNHPIM`, etc.).
- Some institutions mixed both in the same year.

### 2. Asian & Pacific Islander Together (2000–2007)

Early years report Asian / Pacific Islander combined. Modern tables treat:

- **Asian**  
- **Native Hawaiian / Pacific Islander (NHPI)**  

as separate categories.

### 3. Missing or Inconsistent TOTAL Fields

Older tables do not always provide a total per race category (e.g., total men + total women). Some institutions only report gender-specific counts.

### 4. Decimal Precision Issues

Bulk inserts from CSV sometimes imported numeric fields as float with long precision (e.g., `1.099900000000001`).  
Resolved by casting to proper numeric type with controlled scale.

---

## 🛠️ Transformation Rules & Decisions

### ✔️ 1. Handle Mixed Race Coding (2008–2010)

To standardize, we apply:

```sql
COALESCE(CUNKNM, CRACE13) AS unknown_men
COALESCE(CUNKNW, CRACE14) AS unknown_women
```

# IPEDS Data Harmonization

For every race category where the modern variable is missing, fallback to the older equivalent.

Modern-aligned columns are always preferred over legacy ones.

---

## ✔️ 2. Splitting Older Combined Asian/Pacific Islander (2000–2007)

Older years only provide:

- CRACE20 (total)  
- CRACE07 (men)  
- CRACE08 (women)  

**Decision:** Use CRACE07/08 as Asian, and set NHPI columns to `NULL` for these years.

---

## ✔️ 3. Generate Missing Totals

When only men and women exist:

```sql
total_race = men + women
```


This ensures consistency with newer years that always provide totals.

## ✔️ 4. Casting all columns to INTEGER AS PER PostgreSQL Requirement

All columns were casted to INTEGERS before UNION ALL, due to postgreSQL strict rule, some columns have alrdy been casted as iINTEGER soo we can't UNION ALL mixed data type soo we had to cast all columns from this stage
```sql
CAST("CRACE24" AS INTEGER) AS "CTOTALT",
CAST("CRACE15" AS INTEGER) AS "CTOTALM",
CAST("CRACE16" AS INTEGER) AS "CTOTALW"
```

Standard precision = 4 decimals.

## 🔄 Union-All Strategy

After each year is cleaned and aligned to the unified schema:
```sql
SELECT ... FROM c2000_a
UNION ALL
SELECT ... FROM c2001_a
UNION ALL
...
SELECT ... FROM c2023_a
```

### All models enforce:

* consistent column names

* consistent data types

* standardized race structure

### Resulting in a single 6M+ row dataset.

| 🧪 Test Type       | Columns / Notes                        |
|------------------|----------------------------------------|
| not_null          | UNITID, YEAR, CIPCODE                   |
| accepted_range    | Gender counts ≥ 0                       |
| relationships     | UNITID → institutions dimension         |
| unique            | Primary key: (UNITID, YEAR, CIPCODE, race) |

| 🏗️ Column             | Type           | Description                                    |
|-----------------------|---------------|------------------------------------------------|
| UNITID                | INT           | Institution identifier                          |
| YEAR                  | INT           | Reporting year                                  |
| CIPCODE               | DECIMAL(10,4) | Normalized program code                         |
| RACE_CATEGORY         | VARCHAR       | Standardized race category                      |
| MEN                   | INT           | Completions (men)                               |
| WOMEN                 | INT           | Completions (women)                             |
| TOTAL                 | INT           | MEN + WOMEN                                     |
| SOURCE_YEAR_PATTERN   | VARCHAR       | Identifies schema differences (legacy/mixed/modern) |


Real-world IPEDS pipelines face legacy schema mismatches. Harmonization via logic-based rules ensures:

Consistency across multi-decade data

Reproducibility

Transparency of assumptions

Minimal loss of information