WITH deals_raw AS (

     SELECT *
     FROM {{ source('raw_belvo','deals')}}
)


SELECT
     id           AS deal_id
   , externalid   AS external_id
   , ownerid      AS owner_id
   , name         AS deal_name
   , product      AS deal_product
   , amount       AS deal_amount
   , closed       AS deal_closed
   , status       AS deal_status
   , created_date AS deal_created_date
   , closed_1     AS deal_closed_1

FROM deals_raw
