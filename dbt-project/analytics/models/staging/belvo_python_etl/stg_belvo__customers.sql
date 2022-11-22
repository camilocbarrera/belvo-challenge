
WITH customers_raw AS (

     SELECT *

     FROM {{ source('raw_belvo','customers') }}
)

SELECT
     id                                  AS customer_id
   , owner_id                            AS owner_id
   , customer_name                       AS customer_name
   , customer_phase                      AS customer_phase
   , to_timestamp( start_date::integer ) AS start_date
   , end_date                            AS end_date

FROM customers_raw