-- summary of each transaction type over a period of time 
SELECT
  *
FROM (
  SELECT
    DATETIME_TRUNC(created_at, MONTH) date_month,
    SUM(amount)amount,
    type
  FROM (
    SELECT
      DISTINCT id,
      created_at,
      type,
      amount,
      api_id
    FROM
      `buypower-mobile-app.views.api_wallet_transactions`) awt
  WHERE
    awt.created_at BETWEEN "2023-10-05"
    AND "2024-03-14"
    AND awt.api_id = 50
  GROUP BY
    1,
    3 ) PIVOT(SUM(amount) FOR type IN ("transfer",
      "commission",
      "stamp_duty",
      "transfer_topup",
      "refund",
      "vend",
      "reversal")) -- insert distinct types in this place
ORDER BY
  1