SELECT
     stg_belvo__deals.deal_id             AS deal_id
   , stg_belvo__deals.external_id         AS external_id
   , stg_belvo__deals.owner_id            AS owner_id
   , stg_belvo__deals.deal_name           AS deal_name
   , stg_belvo__deals.deal_product        AS deal_product
   , stg_belvo__deals.deal_amount         AS deal_amount
   , stg_belvo__deals.deal_closed         AS deal_closed
   , stg_belvo__deals.deal_status         AS deal_status
   , stg_belvo__deals.deal_created_date   AS deal_created_date
   , stg_belvo__deals.deal_closed_1       AS deal_closed_1
   , stg_belvo__owners.owner_name         AS owner_name
   , stg_belvo__owners.owner_team         AS owner_team
   , stg_belvo__owners.owner_job_position AS owner_job_position

FROM {{ ref('stg_belvo__deals') }} stg_belvo__deals
LEFT JOIN {{ref('stg_belvo__owners') }} stg_belvo__owners
    ON stg_belvo__owners.owner_id = stg_belvo__deals.owner_id
WHERE TRUE