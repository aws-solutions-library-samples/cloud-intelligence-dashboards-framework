-- Account name map derived from the CUR 2.0 export.
-- Returns rows only after AWS delivers the first CUR 2.0 file (~24h after export creation).
-- CUR 2.0 includes line_item_usage_account_name natively (CUR v1 did not),
-- so no extra lookup source is required.
--
-- Run in Athena against the CUR 2.0 database (cid_data_export).

CREATE OR REPLACE VIEW "cid_data_export".account_map AS
SELECT DISTINCT
  line_item_usage_account_id   AS account_id
, line_item_usage_account_name AS account_name
, bill_payer_account_id        AS payer_id
, bill_payer_account_name      AS payer_name
FROM
  "cid_data_export".cur2
WHERE line_item_usage_account_id IS NOT NULL
