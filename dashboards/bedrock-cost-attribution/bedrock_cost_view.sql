CREATE OR REPLACE VIEW ${athena_view_name} AS
SELECT
  bill_billing_period_start_date AS billing_period,
  date_trunc('day', line_item_usage_start_date) AS usage_date,
  bill_payer_account_id AS payer_account_id,
  line_item_usage_account_id AS linked_account_id,
  line_item_product_code AS product_code,
  CASE
    WHEN line_item_usage_type LIKE '%InvokeModel%' THEN 'Model Invocation'
    WHEN line_item_usage_type LIKE '%Provisioned%' THEN 'Provisioned Throughput'
    WHEN line_item_usage_type LIKE '%CustomModel%' THEN 'Custom Model'
    WHEN line_item_usage_type LIKE '%ModelTraining%' THEN 'Model Training'
    WHEN line_item_usage_type LIKE '%KnowledgeBase%' THEN 'Knowledge Base'
    WHEN line_item_usage_type LIKE '%Agent%' THEN 'Agent'
    WHEN line_item_usage_type LIKE '%Guardrail%' THEN 'Guardrail'
    ELSE 'Other'
  END AS usage_category,
  COALESCE(
    REGEXP_EXTRACT(line_item_usage_type, '([^-]+)$'),
    line_item_usage_type
  ) AS model_id,
  CASE
    WHEN line_item_usage_type LIKE '%InputToken%' THEN 'Input Tokens'
    WHEN line_item_usage_type LIKE '%OutputToken%' THEN 'Output Tokens'
    WHEN line_item_usage_type LIKE '%Image%' THEN 'Images'
    ELSE 'Other'
  END AS token_direction,
  line_item_usage_type AS usage_type,
  pricing_unit,
  product['region'] AS region,
  line_item_resource_id AS resource_id,
  line_item_line_item_type AS charge_type,
  ${cur_tags_json} tags_json,
  SUM(line_item_usage_amount) AS usage_amount,
  SUM(line_item_unblended_cost) AS unblended_cost,
  SUM(CASE
    WHEN pricing_unit LIKE '%Token%' THEN line_item_usage_amount
    ELSE 0
  END) AS token_count
FROM
  "${cur2_database}"."${cur2_table_name}"
WHERE
  line_item_product_code IN ('AmazonBedrock', 'AmazonBedrockFoundationModels', 'AmazonBedrockService', 'AmazonBedrockAgentCore')
  AND line_item_line_item_type IN ('Usage', 'DiscountedUsage', 'SavingsPlanCoveredUsage')
  AND line_item_usage_start_date >= date_add('month', -6, current_date)
GROUP BY
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
