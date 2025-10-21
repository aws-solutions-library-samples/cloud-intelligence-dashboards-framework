CREATE OR REPLACE VIEW hourly_view AS
    SELECT DISTINCT
      "line_item_product_code" "product_code"
    , "product_servicecode" "service"
    , ${cur_tags_json} tags_json --replace with ''
    , "line_item_operation" "operation"
    , "line_item_line_item_type" "charge_type"
    , "line_item_usage_type" "usage_type"
    , "line_item_line_item_description" "item_description"
    , "pricing_unit" "pricing_unit"
    , product['region'] "region"
    , "pricing_term" "pricing_term"
    , "bill_billing_period_start_date" "billing_period"
    , "line_item_usage_start_date" "usage_date"
    , "bill_payer_account_id" "payer_account_id"
    , "line_item_usage_account_id" "linked_account_id"
    , "savings_plan_savings_plan_a_r_n" "savings_plan_a_r_n"
    , "reservation_reservation_a_r_n" "reservation_a_r_n"
    , "sum"("line_item_unblended_cost") "unblended_cost"
    , "sum"("reservation_effective_cost") "reservation_effective_cost"
    , "sum"("savings_plan_savings_plan_effective_cost") "savings_plan_effective_cost"
    , "sum"("line_item_usage_amount") "usage_quantity"
    FROM
      "${cur2_database}"."${cur2_table_name}"
    WHERE
      (((current_date - INTERVAL  '30' DAY) <= line_item_usage_start_date)
      AND ((("line_item_line_item_type" = 'Usage') OR ("line_item_line_item_type" = 'SavingsPlanCoveredUsage')) OR ("line_item_line_item_type" = 'DiscountedUsage'))
      AND coalesce("line_item_operation", '') NOT IN ('EKSPod-EC2','ECSTask-EC2'))
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
