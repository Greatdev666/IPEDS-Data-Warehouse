WITH efyyyya_dist AS (
    SELECT * FROM {{ ref('ef2023a_dist') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2022a_dist') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2021a_dist') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2020a_dist') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2019a_dist') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2018a_dist') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2017a_dist') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2016a_dist') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2015a_dist') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2014a_dist') }}
    
    UNION ALL 
    
    SELECT * FROM {{ ref('ef2013a_dist') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2012a_dist') }}

)
SELECT

    "YEAR",
    "UNITID",

    -- Level of Student 
    "EFDELEV",

    -- All students Enrolled
    "EFDETOT",

    -- Students Exclusively Distance Education
    "EFDEEXC",

    -- Students Not Exclusively Distance Education just some courses
    "EFDESOM",

    -- Student not enrolled in any distance education courses
    "EFDENON",

    -- Students enrolled exclusively in distance education courses and are located in same state/jurisdiction as institution
    "EFDEEX1",

    -- Students enrolled exclusively in distance education courses and are located in U.S., not in same state/jurisdiction as institution
    "EFDEEX2",

    -- Students enrolled exclusively in distance education courses and are located in U.S., state/jurisdiction unknown
    "EFDEEX3",

    -- Students enrolled exclusively in distance education courses and are located outside U.S.
    "EFDEEX4",

    -- Students enrolled exclusively in distance education courses and location of student unknown/not reported
    "EFDEEX5"
FROM efyyyya_dist