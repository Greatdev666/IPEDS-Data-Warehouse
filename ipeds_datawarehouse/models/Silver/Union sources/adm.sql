with final_adm AS (
    SELECT * FROM {{ ref('adm2023') }} 
    UNION ALL
    SELECT * FROM {{ ref('adm2022') }}  
    UNION ALL 
    SELECT * FROM {{ ref('adm2021') }}
    UNION ALL 
    SELECT * FROM {{ ref('adm2020') }}
    UNION ALL 
    SELECT * FROM {{ ref('adm2019') }}
    UNION ALL 
    SELECT * FROM {{ ref('adm2018') }}
    UNION ALL 
    SELECT * FROM {{ ref('adm2017') }}
    UNION ALL
    SELECT * FROM {{ ref('adm2016') }}
    UNION ALL 
    SELECT * FROM {{ ref('adm2015') }}
    UNION ALL 
    SELECT * FROM {{ ref('adm2014') }}
)
SELECT 
   "YEAR",
   "UNITID",

    -- Applicants
    "APPLCN",
    "APPLCNM",
    "APPLCNW",
    "APPLCNAN",
    "APPLCNUN",

    -- Admissions
    "ADMSSN",
    "ADMSSNM",
    "ADMSSNW",
    "ADMSSNAN",
    "ADMSSNUN",

    -- Enrolled total
    "ENRLT",
    "ENRLM",
    "ENRLW",
    "ENRLAN",
    "ENRLUN",

    -- Enrolled full-time
    "ENRLFT",
    "ENRLFTM",
    "ENRLFTW",
    "ENRLFTAN",
    "ENRLFTUN",

    -- Enrolled part-time
    "ENRLPT",
    "ENRLPTM",
    "ENRLPTW",
    "ENRLPTAN",
    "ENRLPTUN",

    -- Test scores
    "SATNUM",
    "SATPCT",
    "SATVR25",
    "SATVR50",
    "SATVR75",
    "SATMT25",
    "SATMT50",
    "SATMT75",
    "ACTNUM",
    "ACTPCT",
    "ACTCM25",
    "ACTCM50",
    "ACTCM75",
    "ACTEN25",
    "ACTEN50",
    "ACTEN75",
    "ACTMT25",
    "ACTMT50",
    "ACTMT75"
FROM final_adm
