                   dim_finance_reporting_type
                             │
                             │
                ┌────────────┴─────────────┐
                │                          │
         dim_institution         dim_academic_year
                │                          │
                └────────────┬─────────────┘
                             │
                       fact_finance
                             |
                             |
                   dim_finance_category
                     (metadata only)
