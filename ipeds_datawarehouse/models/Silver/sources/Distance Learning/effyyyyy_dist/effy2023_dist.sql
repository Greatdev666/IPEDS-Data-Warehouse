WITH effy2023a_dist AS (
    SELECT * FROM {{ source('bronze', 'effy2023_dist') }}
)
, effy2023a_dist_renamed AS (
    SELECT
        CAST(2023 AS INTEGER) AS "YEAR",
        CAST("UNITID" AS INTEGER) AS "UNITID",

        -- Level of Student 
        CAST("EFFYDLEV" AS INTEGER) AS "EFFYDLEV",

        -- All students Enrolled
        CAST("EFYDETOT" AS INTEGER) AS "EFYDETOT",

        -- Students Exclusively Distance Education
        CAST("EFYDEEXC" AS INTEGER) AS "EFYDEEXC",

        -- Students Not Exclusively Distance Education just some courses
        CAST("EFYDESOM" AS INTEGER) AS "EFYDESOM",

        -- Student not enrolled in any distance education courses
        CAST("EFYDENON" AS INTEGER) AS "EFYDENON"
        
    FROM effy2023a_dist
)

SELECT * FROM effy2023a_dist_renamed