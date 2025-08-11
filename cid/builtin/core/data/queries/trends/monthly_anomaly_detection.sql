CREATE OR REPLACE VIEW monthly_anomaly_detection AS
SELECT
  "bill_billing_period_start_date"
, "line_item_usage_account_id"
, "account_name"
, (CASE
    WHEN (product['product_name'] LIKE '') THEN "line_item_product_code"
    ELSE product['product_name']
  END) "product_product_name"
, "round"("sum"("line_item_unblended_cost"), 2) "unblended_cost"
, "round"("sum"("line_item_usage_amount"), 2) "line_item_usage_amount"
FROM
  ("${cur2_database}"."${cur2_table_name}"
LEFT JOIN account_map ON ("line_item_usage_account_id" = "account_id"))
WHERE ("date_diff"('month', "date"("bill_billing_period_start_date"), "date"("now"())) <= 20)
GROUP BY "bill_billing_period_start_date", "line_item_usage_account_id", "account_name", 4 