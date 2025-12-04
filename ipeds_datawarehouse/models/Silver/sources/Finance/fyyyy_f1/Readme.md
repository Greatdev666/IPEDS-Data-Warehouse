## 📌 Years Excluded From IPEDS Finance Processing

### ❗ Ignored Years: **2000** and **2001**

These two years were intentionally excluded from the transformation pipeline for the following reasons:

---

### **💥 1. Severe Schema Inconsistencies**
* The 2000 and 2001 finance files use **entirely different column naming patterns** such as:
  * `xa013`, `xj013`, `xb013`, `xh011`, etc.
* Modern IPEDS uses structured identifiers like:
  * `F1A01`, `F1B01`, `F1C011`, `F1D01`, etc.
* Because these early years lack the standardized schema, they **cannot be aligned** through simple renaming or mapping.
* Any Union across years would result in **broken schema alignment** and inconsistent data types.

---

### **🔄 2. Missing or Mismatched Financial Categories**
* Many modern fields are **absent**, such as:
  * Pension & OPEB data
  * Detailed revenues vs. nonoperating revenues
  * Functional expenses broken down (instruction, research, support categories)
  * Endowment beginning/ending balances
* Financial categories are grouped in **completely different structures**, making comparison impossible.

---

### **🚫 3. Not Compatible With Silver/Gold Layer Modeling**
* Because the schema is incompatible, the 2000–2001 files cannot be reliably:
  * Cast to proper data types  
  * Renamed into standardized financial metric names  
  * Joined with later years for trend analysis  
  * Used to compute KPIs in the Gold layer
* Including them would introduce **incorrect financial calculations** and likely break downstream transformations.

---

### **⚙️ 4. Low Analytical Value Compared to Cost of Fixing**
* Manually mapping ~100+ columns for each year provides **very little analytical benefit**.
* Trend analysis (20+ years) remains strong even without these two years.
* Fixing them would create high technical debt with **minimal actual insight gained**.

---

### **📘 Final Decision**
* **Exclude** both `f2000_f1` and `f2001_f1` datasets from:
  * Bronze standardization  
  * Silver data typing & renaming  
  * Gold metrics & dashboards  
* This ensures a **clean, stable, and accurate** financial data pipeline.

---

## 📌 Why We Cast Finance Columns to `NUMERIC` Instead of `BIGINT`, `INTEGER`, or `FLOAT`

### **🎯 Summary**
Financial data from IPEDS should be cast to **NUMERIC** because it preserves precision, supports large values, prevents rounding errors, and matches accounting best practices.

---

### **🧮 1. INTEGER & BIGINT Cannot Store Decimal Values**
* Many financial fields include:
  * Cents (e.g., tuition revenue: `1250000.75`)
  * Fractional calculations (ratios, depreciation allocations)
* INTEGER/BIGINT would **truncate these values** and produce inaccurate financial metrics.

---

### **📏 2. BIGINT Cannot Handle Very Large Values Safely**
* Some institutions report:
  * Assets over **$10 billion**
  * Endowments over **$30 billion**
* BIGINT maximum: **9,223,372,036,854,775,807**
* Although large, accounting systems prefer **precise decimals**, not integers.

---

### **📉 3. FLOAT Causes Rounding Errors (Bad for Finance)**
* FLOAT stores numbers in **scientific notation**, which introduces:
  * Rounding errors  
  * Precision loss  
  * Inaccurate sums over thousands of rows  
* Example error:
  * `1.10 + 2.20 = 3.3000000000000003`
* In finance, this is unacceptable.

---

### **💯 4. NUMERIC Provides Exact Precision (Best for Money)**
* NUMERIC (a.k.a. DECIMAL) stores values **exactly**, with no rounding.
* Perfect for:
  * Revenues  
  * Expenditures  
  * Liabilities  
  * Assets  
  * Scholarships  
  * Endowment values  
  * Ratios or derived KPIs  
* Allows you to specify:
  * **Precision** (total digits)
  * **Scale** (digits after decimal)

---

### **🏛️ 5. Accounting Standards Use DECIMAL/NUMERIC**
* Financial systems (Oracle, SAP, Workday, QuickBooks) all use **Decimal/Numeric**, not float.
* Ensures all transformations match **GASB/FASB standards** and accounting accuracy.

---

### **📘 Final Decision**
* **NUMERIC** is the safest and most accurate data type for IPEDS financial data.
* Guarantees:
  * No rounding errors  
  * Full decimal support  
  * Perfect accounting precision  
  * Compatibility with downstream aggregations and dashboards

---
