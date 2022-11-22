
SELECT
     customer_id
   , owner_id
   , customer_name
   , customer_phase
   , start_date
   , end_date
FROM {{ ref('stg_belvo__customers') }}
WHERE TRUE
