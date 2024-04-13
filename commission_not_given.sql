-- Here, I'm trying to identify wallet transactions that weren't assigned commission. PS: Commissions have the same reference (ref) as the corresponding vend transactions
WITH
  api_trans AS (
  SELECT
    DISTINCT api_id,
    ref,
    STRING_AGG(type,"|"
    ORDER BY
      id ) type_agg, 
    STRING_AGG(DISTINCT token_status,"_") token_status_agg -- essentially joining the types of the same reference together
  FROM (
    SELECT
      DISTINCT awt.api_id,
      awt.ref,
      awt.type,
      awt.id,
    IF
      (tvr.vend_request_id IS NULL, 'FAILED','SUCCESS') token_status,
    FROM
      `[dataset].[table_one_name]` awt
    LEFT JOIN
      `[dataset].[table_two_name]` pt
    ON
      awt.ref = pt.order_id
      AND awt.api_id= pt.api_user_id
    LEFT JOIN
      `[dataset].[table_three_name]` vr
    ON
      pt.id = vr.order_id
    LEFT JOIN
      `[dataset].[table_four_name]` tvr
    ON
      vr.id = tvr.vend_request_id
    JOIN
      `[dataset].[table_five_name]` au
    ON
      awt.api_id = au.id
      AND UPPER(au.type)='PREFUND'
    WHERE
      LOWER(awt.type) IN ("vend",
        "commission")
      AND awt.created_at >= CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AS TIMESTAMP) -- the stakeholder wanted the report to contain data over the past 3 days
    ORDER BY
      awt.id)
  GROUP BY
    1,
    2 )
  -- find transactions with commission not given
SELECT
  *
FROM
  api_trans
WHERE
  type_agg ='vend' -- if it has a commission, type_agg would be 'vend|commission'
  AND token_status_agg = "SUCCESS"