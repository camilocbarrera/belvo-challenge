WITH contacts_raw      AS (
     SELECT *

     FROM {{ source('raw_belvo','contacts') }}

)
   , lookup_countrieas AS (

     SELECT
          country_raw
        , country_code

              FROM {{ ref('country_codes') }}
     WHERE TRUE


)
SELECT DISTINCT
     contacts_raw.id                AS contact_id
   , contacts_raw.name              AS concatct_name
   , contacts_raw.job               AS concatct_job
   , lookup_countrieas.country_code AS country_code
   , contacts_raw.channel           AS contact_channel

FROM contacts_raw
LEFT JOIN lookup_countrieas ON lookup_countrieas.country_raw = contacts_raw.country