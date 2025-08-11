CREATE OR REPLACE VIEW monthly_bill_by_account AS 
WITH
  t1 AS (
   SELECT
     "bill_billing_period_start_date"
   , "bill_payer_account_id"
   , "line_item_usage_account_id"
   , "line_item_line_item_type" "charge_type"
   , (CASE WHEN (product['product_name'] LIKE '') THEN "line_item_product_code" ELSE product['product_name'] END) "product_product_name"
   , product['region'] "product_region"
   , "line_item_product_code"
   , "round"("sum"("line_item_unblended_cost"), 2) "unblended_cost"
   , "round"("sum"((CASE WHEN ("line_item_line_item_type" = 'SavingsPlanCoveredUsage') THEN "savings_plan_savings_plan_effective_cost" WHEN ("line_item_line_item_type" = 'SavingsPlanRecurringFee') THEN ("savings_plan_total_commitment_to_date" - "savings_plan_used_commitment") WHEN ("line_item_line_item_type" = 'SavingsPlanNegation') THEN 0 WHEN ("line_item_line_item_type" = 'SavingsPlanUpfrontFee') THEN 0 WHEN ("line_item_line_item_type" = 'DiscountedUsage') THEN "reservation_effective_cost" WHEN ("line_item_line_item_type" = 'RIFee') THEN ("reservation_unused_amortized_upfront_fee_for_billing_period" + "reservation_unused_recurring_fee") WHEN (("line_item_line_item_type" = 'Fee') AND ("reservation_reservation_a_r_n" <> '')) THEN 0 ELSE "line_item_unblended_cost" END)), 2) "amortized_cost"
   FROM
     "${cur2_database}"."${cur2_table_name}"
   GROUP BY 1, 2, 3, 4, 5, 6, 7
)
, t2 AS (
   SELECT
     "account_name"
   , "account_id"
   , "parent_account_name" "payer_account_name"
   , "parent_account_id"
   FROM
     account_map
)
, t4 AS (
   SELECT
     "region_latitude"
   , "region_longitude"
   , "region_name"
   FROM
     aws_regions
)
, t5 AS (
   SELECT
     "aws_service_category"
   , "line_item_product_code"
   FROM
     aws_service_category_map
)
SELECT
  t1.*
, "t2"."account_name"
, "t2"."payer_account_name"
, TRY_CAST("t3"."region_latitude" AS decimal) "region_latitude"
, TRY_CAST("t3"."region_longitude" AS decimal) "region_longitude"
, (CASE WHEN (("t4"."aws_service_category" IS NULL) AND ("t1"."product_product_name" IS NOT NULL)) THEN "t1"."product_product_name" WHEN (("aws_service_category" IS NULL) AND ("t1"."product_product_name" IS NULL)) THEN 'Other' ELSE "t4"."aws_service_category" END) "aws_service_category"
FROM
  (((t1
LEFT JOIN t2 ON ("t1"."line_item_usage_account_id" = "t2"."account_id" AND "t1"."bill_payer_account_id" = "t2"."parent_account_id"))
LEFT JOIN t3 ON ("t1"."product_region" = "t3"."region_name"))
LEFT JOIN t4 ON ("t1"."line_item_product_code" = "t4"."line_item_product_code"))