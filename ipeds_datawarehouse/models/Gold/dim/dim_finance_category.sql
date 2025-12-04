{{ config(materialized='table') }}
 
WITH finance_category AS (

    -- =============== 1. BALANCE SHEET / ASSETS ===============

    SELECT 1 AS finance_category_sk, 'TOTAL_ASSETS' AS category_code, 'Balance Sheet' AS category_group, 'Assets' AS category_subgroup, 'Total assets' AS description
    UNION ALL SELECT 2, 'TOTAL_CURRENT_ASSETS', 'Balance Sheet', 'Assets', 'Total current assets'
    UNION ALL SELECT 3, 'TOTAL_NONCURRENT_ASSETS', 'Balance Sheet', 'Assets', 'Total non-current assets'
    UNION ALL SELECT 4, 'LONG_TERM_INVESTMENTS', 'Balance Sheet', 'Investments', 'Long-term investments'
    UNION ALL SELECT 5, 'TOTAL_LIABILITIES', 'Balance Sheet', 'Liabilities', 'Total liabilities'
    UNION ALL SELECT 6, 'TOTAL_CURRENT_LIABILITIES', 'Balance Sheet', 'Liabilities', 'Total current liabilities'
    UNION ALL SELECT 7, 'TOTAL_NONCURRENT_LIABILITIES', 'Balance Sheet', 'Liabilities', 'Total non-current liabilities'
    UNION ALL SELECT 8, 'NET_ASSETS_OR_POSITION', 'Balance Sheet', 'Net Position', 'Net assets or net position'
    UNION ALL SELECT 9, 'ENDOWMENT_BEGIN', 'Balance Sheet', 'Endowment', 'Beginning of year endowment value'
    UNION ALL SELECT 10, 'ENDOWMENT_END', 'Balance Sheet', 'Endowment', 'End of year endowment value'
    UNION ALL SELECT 11, 'ENDOWMENT_CHANGE', 'Balance Sheet', 'Endowment', 'Change in endowment value'
    UNION ALL SELECT 12, 'PPE_TOTAL', 'Balance Sheet', 'Property, Plant, Equipment', 'Total property, plant, and equipment'
    UNION ALL SELECT 13, 'PPE_ACCUM_DEPRECIATION', 'Balance Sheet', 'Property, Plant, Equipment', 'Accumulated depreciation'
    UNION ALL SELECT 14, 'PPE_CONSTRUCTION_IN_PROGRESS', 'Balance Sheet', 'Property, Plant, Equipment', 'Construction in progress'
 
    -- ==================== 2. REVENUES ====================

    UNION ALL SELECT 15, 'TUITION_FEES', 'Revenue', 'Operating Revenue', 'Tuition and fees revenue'
    UNION ALL SELECT 16, 'FEDERAL_REVENUE', 'Revenue', 'Government Revenue', 'Federal appropriations or grants revenue'
    UNION ALL SELECT 17, 'STATE_REVENUE', 'Revenue', 'Government Revenue', 'State appropriations or grants revenue'
    UNION ALL SELECT 18, 'PRIVATE_REVENUE', 'Revenue', 'Private Revenue', 'Private gifts, grants, contracts'
    UNION ALL SELECT 19, 'AUXILIARY_ENTERPRISES_REVENUE', 'Revenue', 'Auxiliary Enterprises', 'Auxiliary enterprises revenue'
    UNION ALL SELECT 20, 'SALES_EDUCATIONAL_SERVICES', 'Revenue', 'Operating Revenue', 'Sales and services of educational activities'
    UNION ALL SELECT 21, 'INVESTMENT_INCOME', 'Revenue', 'Investment', 'Investment income'
    UNION ALL SELECT 22, 'HOSPITAL_REVENUE', 'Revenue', 'Operating Revenue', 'Hospital revenue'
    UNION ALL SELECT 23, 'OTHER_REVENUE', 'Revenue', 'Operating Revenue', 'Other operating revenue'
    UNION ALL SELECT 24, 'TOTAL_OPERATING_REVENUE', 'Revenue', 'Operating Revenue', 'Total operating revenue'
    UNION ALL SELECT 25, 'TOTAL_NONOPERATING_REVENUE', 'Revenue', 'Nonoperating Revenue', 'Total non-operating revenue'
    UNION ALL SELECT 26, 'TOTAL_REVENUE', 'Revenue', 'All Revenue', 'Total revenue'

    -- ====================== 3. EXPENSES ======================

    UNION ALL SELECT 27, 'EXPENSE_INSTRUCTION', 'Expense', 'Program Expense', 'Instruction expenses'
    UNION ALL SELECT 28, 'EXPENSE_RESEARCH', 'Expense', 'Program Expense', 'Research expenses'
    UNION ALL SELECT 29, 'EXPENSE_PUBLIC_SERVICE', 'Expense', 'Program Expense', 'Public service expenses'
    UNION ALL SELECT 30, 'EXPENSE_ACADEMIC_SUPPORT', 'Expense', 'Support Expense', 'Academic support expenses'
    UNION ALL SELECT 31, 'EXPENSE_STUDENT_SERVICES', 'Expense', 'Support Expense', 'Student services expenses'
    UNION ALL SELECT 32, 'EXPENSE_INSTITUTIONAL_SUPPORT', 'Expense', 'Support Expense', 'Institutional support expenses'
    UNION ALL SELECT 33, 'EXPENSE_AUXILIARY_ENTERPRISES', 'Expense', 'Auxiliary Enterprises', 'Auxiliary enterprises expenses'
    UNION ALL SELECT 34, 'EXPENSE_SCHOLARSHIPS', 'Expense', 'Scholarships', 'Scholarships and fellowships expenses'
    UNION ALL SELECT 35, 'TOTAL_EXPENSES', 'Expense', 'All Expenses', 'Total expenses'

)

SELECT * FROM finance_category
