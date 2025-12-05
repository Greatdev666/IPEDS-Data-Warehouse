# 📊 IPEDS Analytics Data Marts & Metrics Layer
**Clean Data → Analytical Marts → Business-Ready Metrics**

This document describes the modeling, transformations, and analytical logic behind the **IPEDS Data Marts and Metrics Layer**.  
It highlights business logic, data modeling standards, quality rules, and metrics definitions that power the final analytics environment.

The goal of this layer is to deliver a **unified, reliable, analytics-ready semantic model** for institutional insights across admissions, enrollment, outcomes, finance, and race/ethnicity.

---

## 🧱 Architecture Overview

### **Bronze Layer (Raw Ingestion)**
* 1:1 ingestion of official IPEDS flat files  
* No transformations  
* Ensures full traceability  

### **Silver Layer**
* Standardized schemas  
* Cleaned data types across all years
* Unioned All relative files across years to one big file 


**Gold Layer include (Conformed Facts & Dimensions)**
* Enrollment (full-year)  
* Admissions  
* Completions  
* Outcome / GR  
* Finance  
* etc
---

## **Gold Layer (Analytical Marts + Business Metrics)**  
This README focuses on the Gold Layer, which includes:

* 📘 **mart_admissions**  
* 📙 **mart_completions** & **mart_completions_award_level**  
* 📗 **mart_enrollment**  
* 💰 **mart_finance** & **mart_fact_finance_enriched**  
* 📘 **mart_outcome**  
* 🎨 **mart_race_enrollment**  
* 🎨 **mart_race_completions**  
* 🎨 **mart_race_graduates**
* 🎨 **mart_institution_per_year**

Each mart is fully documented with `schema.yml` and unit-tested for integrity.

---

## 🥇 Data Modeling Standards

### ✔ **Star-Schema Marts**
Each mart follows Kimball-style modeling:

* Explicit fact-table grain  
* Standardized dimensional joins using:  
  * `institutions`  
  * `academic_year`  
  * `award_level`  
  * `finance_category`

### ✔ **Surrogate Keys & Stable PKs**
* `unit_id + academic_year_id` is consistent  
* Natural keys preserved for drill-down  

### ✔ **Best Practices Applied**
* No duplication of grain  
* No fan traps or chasm traps  
* Numeric normalization  
* Divide-by-zero safe metrics  
* Consistent naming conventions  

---

## 🧪 Testing Strategy (`schema.yml`)

### **Column Tests**
* `unique`  
* `not_null`  
* `accepted_values` (categorical fields)  
* `relationships` (FK → dimensions)  
* `dbt_utils.expression_is_true` for logic constraints  

### **Table-Level Tests**
* Grain tests  
* Row-count expectations  
* Null-density checks  
* Year consistency  

### **Metric Tests**
* No negative enrollment  
* YoY % between -100% and +1000%  
* Rates between 0 and 1  

---

## 🔢 Metrics Layer  
Below is a consolidated list of every metric implemented across all marts.  
Optimized for BI tools (Power BI, Tableau, Looker, MetricFlow).

---

## 🚀 Admissions Metrics

### **Category Metrics**
* applicants  
* admissions  
* enrolled  

### **Core Ratios**
* admission_rate  
* yield_rate  

### **YoY**
* applicants_yoy  
* admitted_yoy  
* enrolled_yoy  

### **YoY %**
* applicants_yoy_percent  
* admitted_yoy_percent  
* enrolled_yoy_percent  

_All ratios include divide-by-zero protection._

---

## 📗 Enrollment Metrics (Full-Year)

### **Core**
* total_enrollment  
* total_undergraduates_enrollment  
* total_graduates_enrollment  
* total_male_enrollment  
* total_female_enrollment  

### **YoY Δ (Absolute Change)**
* total_enrollment_yoy_change  
* undergraduate_yoy_change  
* graduate_yoy_change  
* gender-level YoY  

### **YoY %**
* total_enrollment_yoy_percent  
* undergraduate_yoy_percent  
* graduate_yoy_percent  
* male/female_yoy_percent  

---

## 💰 Finance Metrics

### **Core Finance Values**
* total_revenue  
* total_expenses  
* tuition_revenue  
* endowment_value  
* net_position  
* federal_rev  
* state_rev  
* private_rev  

### **Per-Student**
* revenue_per_student  
* expense_per_student  
* endowment_per_student  

### **Operating Ratios**
* operating_margin  
* tuition_dependence_percent  

### **YoY Δ**
* revenue_yoy_change  
* expense_yoy_change  
* endowment_yoy_change  
* margin_yoy_change  
* federal/state/private_yoy_change  

### **YoY %**
* revenue_yoy_percent  
* expense_yoy_percent  
* margin_yoy_percent  
* tuition_dependence_percent_yoy  
* endowment_yoy_percent  

---

## 🎓 Outcomes Metrics (Graduation & Retention)
* Retention rates (1-year, 2-year)  
* Graduation rate 4-year  
* Graduation rate 6-year  
* Graduation rate 8-year  
* Transfer-out rate  
* Completions_100
* Completions_150
* Outcome rate trends YoY  

---

## 🎨 Race Enrollment Metrics

### **Race Categories (IPEDS 9)**
* American Indian or Alaska Native
* Asian  
* Black or Afican American 
* Hispanic or Latino 
* Native Hawaiian or Pacific Islander
* White  
* Two or more races  
* Nonresident alien  
* Unknown race/ethnicity

### **Metrics**
* enrollment by race  
* enrollment % by race  
* undergraduate/graduate by race  
* YoY Δ (absolute)  
* YoY %  

**URM Enrollment Share**  
URM = Black + Hispanic + AIAN + NHPI  / Total Enrollment

**Diversity Index**  
Herfindahl-based score:  
`1 - Σ(race_share²)`

---

## 🎨 Race Completions Metrics
* completions by race  
* completions % of total  
* completions YoY change  
* URM completions share  
* completion pipeline efficiency (completions / enrollment for that race)  

---

## 🎨 Race Graduation Metrics
* Graduation share by race 
* YoY change + % 
* Race gap in graduate share  

---

## 📦 Marts Summary  
Below is the full mart list with business purpose.

### 📘 **mart_admissions**
A cleaned, structured admissions fact including:
* applicants  
* admissions  
* enrolled  
* admission rate  
* yield rate  
* YoY changes  

Optimized for **funnel analytics**.

---

### 📙 **mart_completions**
Aggregated completions at institution + award level grain.

Includes:
* total completions  
* completions by award level  
* STEM vs non-STEM  
* gender distributions  
* YoY changes  

---

### 📙 **mart_completions_award_level**
Same content as `mart_completions` but **fully pivoted by award level categories**.  
Ideal for **program-mix analysis**.

---

### 📗 **mart_enrollment**
Full-year enrollment across institution + year grain.

Includes:
* undergrad, graduate  
* gender splits  
* YoY change  
* YoY percent    

---

### 💰 **mart_fact_finance_enriched**
Intermediate finance fact combining:
* dimensions  
* academic year  
* institution metadata  

---

### 💰 **mart_finance**
Fully pivoted finance mart for financial health analytics.

Includes:
* revenue breakdown  
* expense breakdown  
* per-student metrics  
* operating margin  
* tuition dependence  
* YoY metrics  

Built for **CFO dashboards**.

---

### 📘 **mart_outcome**
Graduation and retention metrics across years.

---

### 🎨 **mart_race_enrollment**
Enrollment counts across all 9 race categories.  
Includes diversity index + URM enrollment share.

---

### 🎨 **mart_race_completions**
Degree completions by race, including pipeline efficiency.

---

### 🎨 **mart_race_graduates**
Graduation rates by race + equity gap metrics.

---

## 🧼 Data Quality Rulebook for Financial KPIs

| Rule | Description |
|------|-------------|
| Revenue ≥ 0 | No negative revenue allowed |
| Expenses ≥ 0 | Negative expenses = data error |
| Margin between -1 and +5 | Prevents unrealistic spikes |
| Tuition dependence ≤ 1 | Tuition / revenue ≤ 100% |
| Endowment per student ≥ 0 | No negative endowment |
| YoY % between -100% and +1000% | Avoids infinite swings |
| Federal/state/private rev sum ≤ total revenue | Components must not exceed total |

Every rule is implemented via **schema tests** or **SQL logic checks**.

---

## 🎯 Value Delivered
This mart layer provides:

* Enterprise-grade analytics  
* Consistent business definitions  
* Unified semantic model  
* Ready for dashboards & ML models  
* High-quality, documented, validated metrics  
* Enables CFO, enrollment, equity, and academic insights  

---

## 👏 Final Notes
This system is built with:

* dbt best practices  
* Full documentation & testing  
* Reusable business metrics  
* Clean dimensional modeling  

**🚀 Ready to transform raw IPEDS data into actionable insights — empowering data analysts to uncover trends, drive decisions, and deliver impact like never before!**






