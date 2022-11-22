WITH companies_cte     AS (
     SELECT *
     FROM {{ source('raw_belvo','companies') }}
     WHERE TRUE

)
   , lookup_countries AS (

     SELECT
          country_raw
        , country_code
    FROM {{ ref('country_codes') }}
     WHERE TRUE


)

SELECT
     companies_cte.id               AS company_id
   , companies_cte.name             AS company_name
   , lookup_countries.country_code  AS country_code

FROM companies_cte
LEFT JOIN lookup_countries ON lookup_countries.country_raw = companies_cte.country
WHERE TRUE
