WITH effyyyyy_dist AS (
    SELECT * FROM {{ ref('effy2023_dist') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2022_dist') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2021_dist') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2020_dist') }}
)

SELECT 
    "YEAR",
    "UNITID",

    -- Level of Student 
    "EFFYDLEV",

    -- All students Enrolled
    "EFYDETOT",

    -- Students Exclusively Distance Education
    "EFYDEEXC",

    -- Students Not Exclusively Distance Education just some courses
    "EFYDESOM",

    -- Student not enrolled in any distance education courses
    "EFYDENON"
FROM effyyyyy_dist