SELECT
     customer_id    AS customer_id
   , owner_id       AS owner_id
   , customer_name  AS customer_name
   , customer_phase AS customer_phase
   , start_date     AS start_date
   , end_date       AS end_date

FROM {{ ref('dim_customers') }}