# IPEDS Modern Analytics Warehouse

*A Complete End-to-End Data Platform for U.S. Higher Education Insights*  
*From 2.6 GB Raw Government Data*  
*24 years Messy Reports to Production-Ready Analytics – Built Solo*
----------
![Architecture Diagram](https://github.com/Greatdev666/IPEDS-Data-Warehouse/blob/master/Architectures/Dataflow%20Overview.png)  
(High-level medallion architecture: Bronze → Silver → Gold → Marts & Metrics → Dashboards. See [dbt docs lineage graph](https://Greatdev666/ipeds-warehouse/lineage.html) for interactive model dependencies.)

## Project Overview
This repository hosts a fully modern, production-grade data warehouse built on the Integrated Postsecondary Education Data System (IPEDS) dataset – one of the largest and most complex public education datasets in the U.S. Spanning 2000–2023, it covers institutional performance, enrollment, completions, finance, and equity metrics for thousands of U.S. higher-education institutions.

Key outcomes:
- *500+ files* processed into a single source of truth.
- *End-to-end ownership*: From raw ingestion to business-ready metrics and team-enabled dashboards.
- *Business value*: Enables CFO-level financial analysis, equity reporting, enrollment forecasting, and graduation funnel optimization.
- *Scale & discipline*: 471 raw files ingested, 200+ transformations, 320+ dbt models, full testing/docs, and collaborative delivery.

This project demonstrates real-world analytics engineering: handling messy legacy data, mid-project migration, and delivering analyst-ready outputs without cutting corners.

- Live dbt docs: [https://your-username.github.io/ipeds-warehouse](https://your-username.github.io/ipeds-warehouse)
- Power BI dashboard previews: coming soon
- BigQuery: [/ipeds_public_datasets](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1sipeds-public!2sipeds_public) (exported CSVs for marts/dims and few facts. Only datasets that are below 100mb are made public)

## Dataset Selection and Initial Analysis
The IPEDS dataset was chosen for its depth, real-world messiness, and high business relevance – covering 2.6 GB of raw Zip files from the U.S. Department of Education (NCES). It includes annual surveys on institutional characteristics, enrollment, completions, finance, and more, making it ideal for demonstrating regulated data handling at scale.

- *Study process*: Reviewed the full IPEDS data dictionary (500+ surveys), analyzed schema changes across 24 years (e.g., naming inconsistencies pre-2011, suppressed values like ".", scientific notation in text fields).
- *Core files selected*: Focused on high-impact surveys for a balanced warehouse:
  - Admissions (ADM)
  - Completions (C): By award level, race/gender, age category
  - Enrollment: Full-year (EFYYYYA) and fall (EFFY)
  - Distance education: Full-year and fall
  - Tuition and fees
  - Salaries and staff
  - Finance (F): Public (F1), nonprofit (F2), for-profit (F3) with FASB/GASB handling
  - Graduation rates (GR) and outcomes
- *Rationale*: Prioritized files enabling cross-domain insights (e.g., enrollment → completions → graduation funnels) while covering equity, finance, and policy analysis. Excluded low-value or redundant surveys to focus on 471 core files.

This selection balanced volume (12.5M+ rows) with analytical depth, simulating enterprise priorities.

## Ingestion Pipeline (Bronze Layer)
Raw data was ingested as-is to preserve fidelity and enable auditing.

- *Initial approach*: Bulk INSERT scripts in SQL Server for fast loading of 471 IPEDS files.
- *Evolution*: Switched to Python for automated ingestion (handling zips, CSVs, schema detection) and file creation. Scripts processed 2.6 GB in batches, with error handling for inconsistent headers.
- *Bronze output*: 471 raw tables, no transformations – pure ELT pattern.

Tech: Python (pandas for schema inference), SQL Server (initial), PostgreSQL (final).

## Transformation Pipeline (Silver Layer)
Transformations were managed with dbt for version control, testing, and modularity.

- *Learning & choice*: Evaluated ETL/ELT tools; chose dbt for its SQL-first approach, Jinja templating, and ecosystem (testing/docs/macros).
- *Key work*: 200+ models to clean, standardize, and union files across years into "big" views (e.g., one unified adm table from 24 years).
- *Challenges handled*: Trailing spaces, inconsistent naming (COALESCE for old/new conventions), data types (casting "[null]" to SQL NULL), scientific notation parsing.
- *Jinja usage*: Macros for reusable logic (e.g., un-pivoting race columns); YAML for schema definitions.

See [Silver README](https://github.com/Greatdev666/IPEDS-Data-Warehouse/blob/master/ipeds_datawarehouse/models/Silver/README.md) for detailed transformations.

## Challenges and Mid-Project Migration
- *SQL Server failure*: Disk space issues mid-silver layer (C: drive full during 471 bronze loads).
- *Migration*: Switched to PostgreSQL for stability; adjusted dbt connection, handled case-sensitivity (quoting columns), fixed invisible spaces in names.
- *Other hurdles*: Date inconsistencies (year-only to full dates), regex for string cleaning (TRIM, UPPER/LOWER, REGEXP_REPLACE), subqueries for aggregations.
- *Resolution*: Full re-run in PostgreSQL; no data loss, gained cross-DB experience.

This simulated real-world infra pivots, strengthening the project's resilience.

## Gold Layer (Facts & Dimensions)
Built a Kimball-style star schema for query efficiency and BI readiness.

- *Output*: 18 models (8 dimensions + 10 facts).
- *Key features*: Surrogate keys, SCD2 snapshots (e.g., dim_institutions), un-pivoting (CROSS JOIN LATERAL for race/ethnicity).
- *Facts*: Enrollment (full-year/race), completions (total/by race/award level), finance (un-pivoted across F1/F2/F3), graduation, admissions.
- *Dimensions*: Institutions, academic year, finance category, award level, race/ethnicity, learning mode.

See [Gold README](https://github.com/Greatdev666/IPEDS-Data-Warehouse/blob/master/ipeds_datawarehouse/models/Gold/Gold_readme.md) for schema diagram and details.

## Marts & Metrics Layer
Created analyst-facing marts with pre-calculated business metrics for drag-and-drop BI.

- *Output*: 18+ marts/models, 100+ metrics (e.g., operating margin, tuition dependence, URM graduation gaps, diversity index via Herfindahl).
- *Categories*: Admissions (yield rate, funnels), enrollment (YoY, per-student), finance (per-student, ratios), outcomes (retention/graduation rates), race breakdowns (equity gaps, pipeline efficiency).
- *Logic*: Window functions for YoY, CASE for safe ratios, aggregations by institution/year/race.

See [Marts & Metrics README](https://github.com/Greatdev666/IPEDS-Data-Warehouse/blob/master/ipeds_datawarehouse/models/Mart/Mart_Readme.md) for full metric definitions and quality rules.

## Tech Stack
- *Ingestion*: Python (automated CSV/Zip processing, pandas for schema handling).
- *Warehouse*: SQL Server (initial), PostgreSQL (final/production).
- *Transformation*: dbt Core (320+ models, 451 macros, Jinja SQL/YAML for templating).
- *Testing/Docs*: dbt tests (uniqueness, not_null, relationships, custom expressions), schema.yml, dbt docs site.
- *Snapshots*: SCD2 for slowly changing dimensions (e.g., institutions).
- *Version Control*: Git/GitHub with branching and commit history.
- *Future-Proof*: ELT pattern; portable to Snowflake/BigQuery.

## Documentation and Artifacts
Detailed layer-specific docs are linked below – no repetition here for brevity.

- [Silver README](https://github.com/Greatdev666/IPEDS-Data-Warehouse/blob/master/ipeds_datawarehouse/models/Silver/README.md): Transformations and union logic.
- [Gold README](https://github.com/Greatdev666/IPEDS-Data-Warehouse/blob/master/ipeds_datawarehouse/models/Gold/Gold_readme.md): Star schema and facts/dims.
- [Marts & Metrics README](https://github.com/Greatdev666/IPEDS-Data-Warehouse/blob/master/ipeds_datawarehouse/models/Mart/Mart_Readme.md): Metric definitions and quality rules.
- dbt Docs: [Interactive Lineage](https://your-username.github.io/ipeds-warehouse).
- Diagrams: Architecture (above), star schema in Gold README.
- Exports: CSVs for marts/dims in [ipeds_public](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1sipeds-public!2sipeds_public) or Google BigQuery public dataset (your-project.ipeds_public).

## How to Run/Reproduce
1. Clone repo: git clone https://greatdev666.github.io/IPEDS-Data-Warehouse.
2. Install dbt + PostgreSQL.
3. Download raw dataset from https://zenodo.org/records/15028111
4. Select files available in ([sources.yml](https://github.com/Greatdev666/IPEDS-Data-Warehouse/blob/master/ipeds_datawarehouse/models/Silver/bronze.yml)).
5. Run ingestion scripts in [/Bonze](https://github.com/Greatdev666/IPEDS-Data-Warehouse/blob/master/Bronze/connection_and_loading_bronze.ipynb) (Python).
6. dbt seed && dbt run for bronze/silver/gold.
7. dbt test for validation.
8. Connect BI tool to PostgreSQL or use exported CSVs.

## Future Enhancements
- Integrate ML (e.g., graduation prediction).
- Add Airflow for orchestration.
- Expand to full IPEDS (e.g., HR surveys).
- Cloud migration demo (already tested on Snowflake).

## About Me
Self-taught analytics engineer with a passion for turning messy data into business impact. This project replaced traditional internships by demonstrating full-stack delivery and team enablement. Open to Analytics Engineer/Data Engineer roles (remote preferred).

LinkedIn: [Muhammad Bashir](#)  

Github: [Muhammad Bashir](https://github.com/Greatdev666)

Email: codedelex@gmail.com 

Thanks for checking out the repo – let's connect if you're building something big! 🚀
