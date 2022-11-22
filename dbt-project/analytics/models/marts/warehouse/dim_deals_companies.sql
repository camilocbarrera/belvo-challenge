SELECT
     stg_belvo__companies.country_code                  AS country_code
   , stg_belvo__companies_deals_associations.company_id AS company_id
   , stg_belvo__companies_deals_associations.deal_id    AS deal_id
   , stg_belvo__companies.company_name                  AS company_name


FROM {{ ref('stg_belvo__companies_deals_associations') }} stg_belvo__companies_deals_associations
LEFT JOIN {{ref('stg_belvo__companies')}} stg_belvo__companies
    ON stg_belvo__companies.company_id = stg_belvo__companies_deals_associations.company_id
WHERE TRUE
