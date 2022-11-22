

WITH wide_mart_denormalize AS (
     SELECT
          fct_deals_owners.deal_id            AS deal_id
        , fct_deals_owners.external_id        AS external_id
        , fct_deals_owners.owner_id           AS owner_id
        , fct_deals_owners.deal_name          AS deal_name
        , fct_deals_owners.deal_product       AS deal_product
        , fct_deals_owners.deal_amount        AS deal_amount
        , fct_deals_owners.deal_closed        AS deal_closed
        , fct_deals_owners.deal_status        AS deal_status
        , fct_deals_owners.deal_created_date  AS deal_created_date
        , fct_deals_owners.deal_closed_1      AS deal_closed_1
        , fct_deals_owners.owner_name         AS owner_name
        , fct_deals_owners.owner_team         AS owner_team
        , fct_deals_owners.owner_job_position AS owner_job_position

        , dim_deals_companies.country_code    AS deals_companies_country_code
        , dim_deals_companies.company_id      AS company_id

        , dim_deals_companies.company_name    AS deals_contacts_company_name
        , dim_deals_contacts.country_code     AS deals_contacts_country_code
        , dim_deals_contacts.contact_id       AS deals_contacts_contact_id
        , dim_deals_contacts.concatct_name    AS deals_contacts_concatct_name
        , dim_deals_contacts.concatct_job     AS deals_contacts_concatct_job
        , dim_deals_contacts.contact_channel  AS deals_contacts_contact_channel

        , dim_customers.customer_id           AS customer_id
        , dim_customers.owner_id              AS customer_owner_id
        , dim_customers.customer_name         AS customer_name
        , dim_customers.customer_phase        AS customer_phase
        , dim_customers.start_date            AS customer_start_date
        , dim_customers.end_date              AS customer_end_date

     FROM {{ ref('fct_deals_owners') }} fct_deals_owners
     LEFT JOIN {{ ref('dim_deals_companies') }} dim_deals_companies ON dim_deals_companies.deal_id = fct_deals_owners.deal_id
     LEFT JOIN {{ ref('dim_deals_contacts') }} dim_deals_contacts ON dim_deals_contacts.deal_id = fct_deals_owners.deal_id
     LEFT JOIN {{ ref('dim_customers') }} dim_customers ON dim_customers.customer_id = fct_deals_owners.external_id

)
SELECT *
FROM wide_mart_denormalize