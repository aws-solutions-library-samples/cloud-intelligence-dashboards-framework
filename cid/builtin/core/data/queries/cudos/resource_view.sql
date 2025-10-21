CREATE OR REPLACE VIEW resource_view AS
      SELECT DISTINCT
        "date_trunc"('day', "line_item_usage_start_date") "usage_date"
      , "bill_payer_account_id" "payer_account_id"
      , ${cur_tags_json} tags_json --replace with ''
      , "line_item_usage_account_id" "linked_account_id"
      , "bill_billing_entity" "billing_entity"
      , product['product_name'] "product_name"
      , "line_item_resource_id" "resource_id"
      , "line_item_product_code" "product_code"
      , "line_item_operation" "operation"
      , "line_item_line_item_type" "charge_type"
      , "line_item_usage_type" "usage_type"
      , "pricing_unit" "pricing_unit"
      , product['region'] "region"
      , "line_item_line_item_description" "item_description"
      , "line_item_legal_entity" "legal_entity"
      , "pricing_term" "pricing_term"
      , product['database_engine'] "database_engine"
      , product['deployment_option'] "product_deployment_option"
      , "product_from_location" "product_from_location"
      , product['group'] "product_group"
      , "product_instance_type" "instance_type"
      , product['instance_type_family'] "instance_type_family"
      , product['operating_system'] "platform"
      , "product_product_family" "product_family"
      , "product_servicecode" "service"
      , product['storage'] "product_storage"
      , "product_to_location" "product_to_location"
      , product['volume_api_name'] "product_volume_api_name"
      , "reservation_reservation_a_r_n" "reservation_a_r_n"
      , "savings_plan_savings_plan_a_r_n" "savings_plan_a_r_n"
      , "sum"("savings_plan_savings_plan_effective_cost") "savings_plan_effective_cost"
      , "sum"("reservation_effective_cost") "reservation_effective_cost"
      , "sum"("line_item_usage_amount") "usage_quantity"
      , "sum"("line_item_unblended_cost") "unblended_cost"
      FROM
        "${cur2_database}"."${cur2_table_name}"
      WHERE
        (((current_date - INTERVAL  '30' DAY) <= line_item_usage_start_date)
        AND (line_item_resource_id <> '')
        AND coalesce("line_item_operation", '') NOT IN ('EKSPod-EC2','ECSTask-EC2'))
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30
