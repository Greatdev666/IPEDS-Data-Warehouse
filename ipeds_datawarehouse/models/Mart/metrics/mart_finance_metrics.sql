WITH base AS (
    SELECT * FROM {{ ref('mart_finance') }}
),
agg AS (
    SELECT
        b.*,
        -- ===== Per Student Metrics =====
        (b.total_revenue / NULLIF(b.total_enrolled, 0)) AS revenue_per_student,
        (b.total_expenses / NULLIF(b.total_enrolled, 0)) AS expense_per_student,
        (b.endowment_value / NULLIF(b.total_enrolled, 0)) AS endowment_per_student,

        -- Operating Margin
        ((b.total_revenue - b.total_expenses) / NULLIF(total_revenue, 0)) AS operating_margin,

         -- Tuition Dependence %
        (b.tuition_revenue / NULLIF(total_revenue,0)) AS tution_dependence_percent
    FROM base b 
),
yoy AS (
    SELECT 
        a.*,
        -- Revenue YoY change
        a.total_revenue -
        LAG(a.total_revenue) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) AS revenue_yoy_change,
        CASE 
         WHEN LAG(a.total_revenue) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) = 0
         THEN NULL
         ELSE 
            (a.total_revenue::decimal
                / LAG(a.total_revenue) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id)
            ) - 1
        END revenue_yoy_percent,

        -- Revenue per student YoY change
        a.revenue_per_student -
        LAG(a.revenue_per_student) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) AS revenue_per_student_yoy_change,
        CASE 
         WHEN LAG(a.revenue_per_student) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) = 0
         THEN NULL
         ELSE 
            (a.revenue_per_student::decimal
                / LAG(a.revenue_per_student) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id)
            ) - 1
        END revenue_per_student_yoy_percent,

        -- total expenses YoY change
        a.total_expenses -
        LAG(a.total_expenses) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) AS total_expenses_yoy_change,
        CASE 
         WHEN LAG(a.total_expenses) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) = 0
         THEN NULL
         ELSE 
            (a.total_expenses::decimal
                / LAG(a.total_expenses) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id)
            ) - 1
        END total_expenses_yoy_percent,

        -- expense_per_student YoY change
        a.expense_per_student -
        LAG(a.expense_per_student) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) AS expense_per_student_yoy_change,
        CASE 
         WHEN LAG(a.expense_per_student) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) = 0
         THEN NULL
         ELSE 
            (a.expense_per_student::decimal
                / LAG(a.expense_per_student) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id)
            ) - 1
        END expense_per_student_yoy_percent,

        -- expense_per_student YoY change
        a.endowment_value -
        LAG(a.endowment_value) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) AS endowment_value_yoy_change,
        CASE 
         WHEN LAG(a.endowment_value) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) = 0
         THEN NULL
         ELSE 
            (a.endowment_value::decimal
                / LAG(a.endowment_value) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id)
            ) - 1
        END endowment_value_yoy_percent,

        -- endowment per student YoY change
        a.endowment_per_student -
        LAG(a.endowment_per_student) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) AS endowment_per_student_yoy_change,
        CASE 
         WHEN LAG(a.endowment_per_student) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) = 0
         THEN NULL
         ELSE 
            (a.endowment_per_student::decimal
                / LAG(a.endowment_per_student) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id)
            ) - 1
        END endowment_per_student_yoy_percent,

        -- Net position YoY change
        a.net_position -
        LAG(a.net_position) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) AS net_position_yoy_change,
        CASE 
         WHEN LAG(a.net_position) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) = 0
         THEN NULL
         ELSE 
            (a.net_position::decimal
                / LAG(a.net_position) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id)
            ) - 1
        END net_position_yoy_percent,

        -- Tuition revenue YoY change
        a.tuition_revenue -
        LAG(a.tuition_revenue) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) AS tuition_revenue_yoy_change,
        CASE 
         WHEN LAG(a.tuition_revenue) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) = 0
         THEN NULL
         ELSE 
            (a.tuition_revenue::decimal
                / LAG(a.tuition_revenue) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id)
            ) - 1
        END tuition_revenue_yoy_percent,

        -- Operating margin YoY change
        a.operating_margin -
        LAG(a.operating_margin) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) AS operating_margin_yoy_change,
        CASE 
         WHEN LAG(a.operating_margin) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) = 0
         THEN NULL
         ELSE 
            (a.operating_margin::decimal
                / LAG(a.operating_margin) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id)
            ) - 1
        END operating_margin_yoy_percent,

        -- Federal revenue YoY change
        a.federal_rev -
        LAG(a.federal_rev) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) AS federal_rev_yoy_change,
        CASE 
         WHEN LAG(a.federal_rev) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) = 0
         THEN NULL
         ELSE 
            (a.federal_rev::decimal
                / LAG(a.federal_rev) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id)
            ) - 1
        END federal_rev_yoy_percent,

        -- State revenue YoY change
        a.state_rev -
        LAG(a.state_rev) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) AS state_rev_yoy_change,
        CASE 
         WHEN LAG(a.state_rev) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) = 0
         THEN NULL
         ELSE 
            (a.state_rev::decimal
                / LAG(a.state_rev) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id)
            ) - 1
        END state_rev_yoy_percent,

        -- Private revenue YoY change
        a.private_rev -
        LAG(a.private_rev) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) AS private_rev_yoy_change,
        CASE 
         WHEN LAG(a.private_rev) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id) = 0
         THEN NULL
         ELSE 
            (a.private_rev::decimal
                / LAG(a.private_rev) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id)
            ) - 1
        END private_rev_yoy_percent

    FROM agg a 
)

SELECT * FROM yoy 