WITH companies_deals_associations_cte AS (
     SELECT *

     FROM {{ source('raw_belvo','companies_deals_associations') }}
)


SELECT DISTINCT

     companyid               AS company_id
   , deal_ids.value::integer AS deal_id


FROM                                                                      companies_deals_associations_cte
   , LATERAL flatten( INPUT => PARSE_JSON( dealids::variant )::ARRAY ) AS deal_ids