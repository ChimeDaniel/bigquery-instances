  -- Select all needed columns and the values of the row before each row
WITH
  wallets AS(
  SELECT
    DISTINCT *,
    -- select row before row using LEAD
    LEAD(balance_after) OVER(PARTITION BY api_id ORDER BY id DESC) balance_after_prev,
    LEAD(balance_before) OVER(PARTITION BY api_id ORDER BY id DESC) balance_before_prev,
    LEAD(created_at) OVER(PARTITION BY api_id ORDER BY id DESC) created_at_prev,
    LEAD(id) OVER(PARTITION BY api_id ORDER BY id DESC) id_prev,
    LEAD(ref) OVER(PARTITION BY api_id ORDER BY id DESC) ref_prev,
    LEAD(type) OVER (PARTITION BY api_id ORDER BY id DESC) type_prev,
    LEAD(operation) OVER (PARTITION BY api_id ORDER BY id DESC) operation_prev,
    LEAD(amount) OVER(PARTITION BY api_id ORDER BY id DESC) amount_due,
    LEAD(status) OVER(PARTITION BY api_id ORDER BY id DESC) status_prev,
    LEAD(
    IF
      (balance_after > balance_before, amount, -amount)) OVER(PARTITION BY api_id ORDER BY id DESC) amount_prev
  FROM (
    SELECT
      DISTINCT au.name,
      awt.*,
    IF
      (tvr.payment_reference IS NULL,'FAILED','SUCCESS') status
    -- table names and datasets are abstracted to protect company privacy
    FROM
      `[dataset_name].[table_one_name]` awt
    LEFT JOIN
      `[dataset_name].[table_two_name]` pt
    ON
      awt.ref = pt.order_id
    LEFT JOIN
      `[dataset_name].[table_three_name]` tvr
    ON
      pt.order_id = tvr.payment_reference
    LEFT JOIN
      `[dataset_name].[table_four_name]` au
    ON
      awt.api_id = au.id
      -- table conditions
    WHERE
      au.type='PREFUND'
      AND awt.type NOT IN ('commission',
        'commssion')
      AND awt.created_at BETWEEN "2022-01-01"
      AND "2022-12-31 23:59:59"
      AND awt.api_id = 24 ) ),

  -- Perform aggregations
  wallets_unaffected AS (
  SELECT
    name,
    api_id,
    created_at,
    id,
    id_prev,
    ref_prev,
    type_prev,
    amount_prev,
    status_prev,
    balance_after_prev-balance_before diff,
  IF
    (balance_after_prev-balance_before=amount_prev,'NOT COMM','COMM') is_hitting,
  FROM
    wallets
  WHERE
    balance_after_prev != balance_before -- this is the most important clause. If this condition is true, the transaction did not affect the wallet
  ORDER BY
    id DESC ),
  Final AS (
  SELECT
    wu.*,
    DATE_TRUNC(created_at, MONTH) date_month
  FROM
    wallets_unaffected wu
    )
SELECT
  *
FROM
  final 
WHERE 
  final.is_hitting = "COMM" -- Or final.is_hitting = "NOT COMM" depending on what set of data you would like to see.