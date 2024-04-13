-- In this project, I extract data from logs gotten from firebase. These are logs of aquired Merchant details submitted by various field inspection agents.
WITH
  merchant_view AS ( -- extract required details from logs
  SELECT
    SUBSTR(JSON_EXTRACT(DATA, "$.reference"),2, LENGTH(JSON_EXTRACT(DATA, "$.reference"))-2) reference,
    CAST(SUBSTR(JSON_EXTRACT(DATA, "$.createdAt"),2, LENGTH(JSON_EXTRACT(DATA, "$.createdAt"))-2) AS timestamp) createdAt,
    SUBSTR(JSON_EXTRACT(DATA, "$.agent"),2, LENGTH(JSON_EXTRACT(DATA, "$.agent"))-2) agent,
    SUBSTR(JSON_EXTRACT(DATA, "$.projectKey"),2, LENGTH(JSON_EXTRACT(DATA, "$.projectKey"))-2) projectKey,
    SUBSTR(JSON_EXTRACT(DATA, "$.projectName"),2, LENGTH(JSON_EXTRACT(DATA, "$.projectName"))-2) projectName,
    JSON_EXTRACT(DATA, "$.synced") synced,
    JSON_EXTRACT(DATA, "$.id") id,
    SUBSTR(JSON_EXTRACT(DATA, "$.data.merchantPhoneNumber"),2, LENGTH(JSON_EXTRACT(DATA, "$.data.merchantPhoneNumber"))-2) merchantPhoneNumber,
    SUBSTR(JSON_EXTRACT(DATA, "$.data.merchantName"),2, LENGTH(JSON_EXTRACT(DATA, "$.data.merchantName"))-2) merchantName,
    SUBSTR(JSON_EXTRACT(DATA, "$.data.merchantEmailAddress"),2, LENGTH(JSON_EXTRACT(DATA, "$.data.merchantEmailAddress"))-2) merchantEmailAddress
  FROM
    `[project_name].firestore_collections.[table_name]` -- table and project name are abstracted to protect company privacy
  WHERE
    SUBSTR(JSON_EXTRACT(DATA, "$.projectKey"),2, LENGTH(JSON_EXTRACT(DATA, "$.projectKey"))-2) = "merchant-acquisition" )

-- I realised that some agents where submitting merchant details that had already been submitted so I'm only picking the details submitted first
SELECT
  DISTINCT
IF
  (LEFT(merchantPhoneNumber,1) <> "0", CONCAT("0", RIGHT(REPLACE(replace(merchantPhoneNumber,
            "‬",
            "")," ",""),10)),REPLACE(merchantPhoneNumber," ","")) merchantPhoneNumber, -- I observed some issues with the numbers, so this is me trying to correctly format it
  FIRST_VALUE(merchantName) OVER (PARTITION BY merchantPhoneNumber ORDER BY createdAt ASC) first_merchant_name,
  FIRST_VALUE(merchantEmailAddress) OVER (PARTITION BY merchantPhoneNumber ORDER BY createdAt ASC) first_merchant_email,
  FIRST_VALUE(createdAt) OVER (PARTITION BY merchantPhoneNumber ORDER BY createdAt ASC) first_seen_date,
  FIRST_VALUE(agent) OVER(PARTITION BY merchantPhoneNumber ORDER BY createdAt ASC) first_agent
FROM
  merchant_view