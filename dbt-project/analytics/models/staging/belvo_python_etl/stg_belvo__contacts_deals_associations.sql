
WITH contacts_deals_associations_raw AS (

     SELECT *

     FROM {{ source('raw_belvo','contacts_deals_associations') }}
)

SELECT DISTINCT

     contactid               AS contact_id
   , deal_ids.value::integer AS deal_id

FROM                                                                      contacts_deals_associations_raw
   , LATERAL flatten( INPUT => PARSE_JSON( dealids::variant )::ARRAY ) AS deal_ids
