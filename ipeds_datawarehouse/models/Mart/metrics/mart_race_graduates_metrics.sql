WITH base AS (
    SELECT * FROM {{ ref('mart_race_graduates') }}
), 
yoy AS (
    SELECT
        -- YoY absolute change 
        b.*,

        b.total_graduates - 
            LAG(b.total_graduates) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS total_graduates_yoy_change,

        -- Race breakdown yoy change
            
        b.white_graduates - 
            LAG(b.white_graduates) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS white_graduates_yoy_change,
            
        b.black_or_african_graduates - 
            LAG(b.black_or_african_graduates) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS black_or_african_graduates_yoy_change,
            
        b.hispanic_or_latino_graduates - 
            LAG(b.hispanic_or_latino_graduates) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS hispanic_or_latino_graduates_yoy_change,
            
        b.asian_graduates - 
            LAG(b.asian_graduates) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS asian_graduates_yoy_change,
            
        b.american_indian_or_alaska_native_graduates - 
            LAG(b.american_indian_or_alaska_native_graduates) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS american_indian_or_alaska_native_graduates_yoy_change,
            
        b.pacific_islander_or_native_hawaiian_graduates - 
            LAG(b.pacific_islander_or_native_hawaiian_graduates) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS pacific_islander_or_native_hawaiian_graduates_yoy_change,
            
        b.two_or_more_races_graduates - 
            LAG(b.two_or_more_races_graduates) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS two_or_more_races_graduates_yoy_change,
            
        b.nonresident_alien_graduates - 
            LAG(b.nonresident_alien_graduates) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS nonresident_alien_graduates_yoy_change,

        b.unknown_race_graduates - 
            LAG(b.unknown_race_graduates) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS unknown_race_graduates_yoy_change,

        -- YoY percent change
        CASE 
            WHEN LAG(b.total_graduates) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.total_graduates::decimal
                   / LAG(b.total_graduates) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END graduates_yoy_percent,
        
        --Race break down yoy % change

        CASE 
            WHEN LAG(b.white_graduates) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.white_graduates::decimal
                   / LAG(b.white_graduates) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END white_graduates_yoy_percent,

        CASE 
            WHEN LAG(b.black_or_african_graduates) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.black_or_african_graduates::decimal
                   / LAG(b.black_or_african_graduates) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END black_or_african_graduates_yoy_percent,

        CASE 
            WHEN LAG(b.hispanic_or_latino_graduates) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.hispanic_or_latino_graduates::decimal
                   / LAG(b.hispanic_or_latino_graduates) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END hispanic_or_latino_graduates_yoy_percent,

        CASE 
            WHEN LAG(b.asian_graduates) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.asian_graduates::decimal
                   / LAG(b.asian_graduates) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END asian_graduates_yoy_percent,

        CASE 
            WHEN LAG(b.american_indian_or_alaska_native_graduates) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.american_indian_or_alaska_native_graduates::decimal
                   / LAG(b.american_indian_or_alaska_native_graduates) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END american_indian_or_alaska_native_graduates_yoy_percent,

        CASE 
            WHEN LAG(b.pacific_islander_or_native_hawaiian_graduates) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.pacific_islander_or_native_hawaiian_graduates::decimal
                   / LAG(b.pacific_islander_or_native_hawaiian_graduates) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END pacific_islander_or_native_hawaiian_graduates_yoy_percent,

        CASE 
            WHEN LAG(b.two_or_more_races_graduates) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.two_or_more_races_graduates::decimal
                   / LAG(b.two_or_more_races_graduates) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END two_or_more_races_graduates_yoy_percent,

        CASE 
            WHEN LAG(b.nonresident_alien_graduates) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.nonresident_alien_graduates::decimal
                   / LAG(b.nonresident_alien_graduates) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END nonresident_alien_graduates_yoy_percent,

        CASE 
            WHEN LAG(b.unknown_race_graduates) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.unknown_race_graduates::decimal
                   / LAG(b.unknown_race_graduates) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END unknown_race_graduates_yoy_percent
    FROM base b
),
race_percent_share_of_institution AS (
    SELECT 
        y.*,
        
        -- Race Share of Institution
        CASE WHEN y.total_graduates = 0 THEN NULL
        ELSE y.white_graduates::decimal / y.total_graduates END AS white_share,

        CASE WHEN y.total_graduates = 0 THEN NULL
        ELSE y.black_or_african_graduates::decimal / y.total_graduates END AS black_share,

        CASE WHEN y.total_graduates = 0 THEN NULL
        ELSE y.hispanic_or_latino_graduates::decimal / y.total_graduates END AS hispanic_share,

        CASE WHEN y.total_graduates = 0 THEN NULL
        ELSE y.asian_graduates::decimal / y.total_graduates END AS asian_share,

        CASE WHEN y.total_graduates = 0 THEN NULL
        ELSE y.american_indian_or_alaska_native_graduates::decimal / y.total_graduates END AS native_share,

        CASE WHEN y.total_graduates = 0 THEN NULL
        ELSE y.pacific_islander_or_native_hawaiian_graduates::decimal / y.total_graduates END AS pacific_share,

        CASE WHEN y.total_graduates = 0 THEN NULL
        ELSE y.two_or_more_races_graduates::decimal / y.total_graduates END AS two_or_more_share,

        CASE WHEN y.total_graduates = 0 THEN NULL
        ELSE y.nonresident_alien_graduates::decimal / y.total_graduates END AS nonresident_share,

        CASE WHEN y.total_graduates = 0 THEN NULL
        ELSE y.unknown_race_graduates::decimal / y.total_graduates END AS unknown_share

    FROM yoy y 
),
race_gap_metrics AS (
    SELECT 
    r.*,
    -- Race gap metrics
    -- Race Gaps
    (black_share - white_share) AS black_white_gap,
    (hispanic_share - white_share) AS hispanic_white_gap,
    (asian_share - white_share) AS asian_white_gap,
    (native_share - white_share) AS native_white_gap,
    (pacific_share - white_share) AS pacific_white_gap,
    (two_or_more_share - white_share) AS two_or_more_white_gap

    FROM race_percent_share_of_institution r
)

SELECT * FROM race_gap_metrics