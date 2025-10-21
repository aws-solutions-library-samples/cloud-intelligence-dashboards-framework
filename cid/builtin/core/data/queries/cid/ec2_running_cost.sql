 CREATE OR REPLACE VIEW "ec2_running_cost" AS
 SELECT DISTINCT
    split_part("billing_period", '-', 1) "year"
 ,  split_part("billing_period", '-', 2) "month"
 , ${cur_tags_json} tags_json --replace with
 , "bill_billing_period_start_date" "billing_period"
 ,  "date_trunc"('hour', "line_item_usage_start_date") "usage_date"
 , "bill_payer_account_id" "payer_account_id"
 , "line_item_usage_account_id" "linked_account_id"
 , (CASE
     WHEN ("savings_plan_savings_plan_a_r_n" <> '') THEN 'SavingsPlan'
     WHEN ("reservation_reservation_a_r_n" <> '') THEN 'Reserved'
     WHEN ("line_item_usage_type" LIKE '%Spot%') THEN 'Spot'
     ELSE 'OnDemand'
    END) "purchase_option"
 , "sum"(CASE
     WHEN "line_item_line_item_type" = 'SavingsPlanCoveredUsage' THEN "savings_plan_savings_plan_effective_cost"
     WHEN "line_item_line_item_type" = 'DiscountedUsage' THEN "reservation_effective_cost"
     WHEN "line_item_line_item_type" = 'Usage' THEN "line_item_unblended_cost"
     ELSE 0 END) "amortized_cost"
 , "round"("sum"("line_item_usage_amount"), 2) "usage_quantity"
 FROM
    "${cur2_database}"."${cur2_table_name}"
 WHERE (((((("bill_billing_period_start_date" >= ("date_trunc"('month', current_timestamp) - INTERVAL  '1' MONTH))
  AND ("line_item_product_code" = 'AmazonEC2'))
  AND ("product_servicecode" <> 'AWSDataTransfer'))
  AND ("line_item_operation" LIKE '%RunInstances%'))
  AND ("line_item_usage_type" NOT LIKE '%DataXfer%'))
  AND ((("line_item_line_item_type" = 'Usage')
     OR ("line_item_line_item_type" = 'SavingsPlanCoveredUsage'))
     OR ("line_item_line_item_type" = 'DiscountedUsage')))
 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
