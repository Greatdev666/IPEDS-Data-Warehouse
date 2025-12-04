WITH base AS (
    SELECT * FROM {{ ref('mart_completions') }}
), 
agg AS (
    SELECT 
        unit_id,
        academic_year_id,
        institution_name,
        citY,
        state_abbreviation,
        control_type,
        sector,
        institution_level,

        -- Total completions
        SUM(total_completions) as total_completions,

        --Gender Split
        SUM(total_male_completions) AS total_male_completions,
        SUM(total_female_completions) AS total_female_completions
    FROM base
    GROUP BY 1,2,3,4,5,6,7,8 
),
yoy AS (
    SELECT
        -- YoY absolute change 
        a.*,
        a.total_completions - 
            LAG(a.total_completions) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id)
            AS completions_yoy_change,
        -- YoY percent change
        CASE 
            WHEN LAG(a.total_completions) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (a.total_completions::decimal
                   / LAG(a.total_completions) OVER(
                    PARTITION BY a.unit_id ORDER BY a.academic_year_id
                    )
                ) - 1 
        END completions_yoy_percent,

        --Gender %
        CASE 
            WHEN a.total_completions = 0 THEN NULL 
            ELSE a.total_male_completions::decimal / a.total_completions
        END male_completions_percent,

        CASE 
            WHEN a.total_completions = 0 THEN NULL 
            ELSE a.total_female_completions::decimal / a.total_completions
        END female_completions_percent,


        --Gender YoY absolute change

        a.total_male_completions - 
        LAG(a.total_male_completions) OVER(
            PARTITION BY a.unit_id ORDER BY a.academic_year_id
        )  AS male_completions_yoy_change,

        a.total_female_completions - 
        LAG(a.total_female_completions) OVER(
            PARTITION BY a.unit_id ORDER BY a.academic_year_id
        ) AS female_completions_yoy_change,

        -- Gender YoY %
        CASE 
            WHEN LAG(a.total_male_completions) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
            (a.total_male_completions::decimal
               / LAG(a.total_male_completions) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
                ) 
            )- 1
        END male_completions_yoy_percent,

         CASE 
            WHEN LAG(a.total_female_completions) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
            (a.total_female_completions::decimal
               / LAG(a.total_female_completions) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
                )
            ) - 1
        END female_completions_yoy_percent
           
    FROM agg a
)

SELECT * FROM yoy