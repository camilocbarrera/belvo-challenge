SELECT
     stg_belvo__contacts.country_code                  AS country_code
   , stg_belvo__contacts_deals_associations.contact_id AS contact_id
   , stg_belvo__contacts_deals_associations.deal_id    AS deal_id
   , stg_belvo__contacts.concatct_name                 AS concatct_name
   , stg_belvo__contacts.concatct_job                  AS concatct_job
   , stg_belvo__contacts.contact_channel               AS contact_channel

FROM {{ ref('stg_belvo__contacts_deals_associations') }} stg_belvo__contacts_deals_associations
LEFT JOIN {{ ref('stg_belvo__contacts') }}
    ON stg_belvo__contacts.contact_id = stg_belvo__contacts_deals_associations.contact_id
WHERE TRUE
