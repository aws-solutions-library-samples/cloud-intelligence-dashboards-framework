CREATE OR REPLACE VIEW "s3_view" AS
SELECT DISTINCT 
    split_part("billing_period", '-', 1) "year",
    split_part("billing_period", '-', 2) "month",
    ${cur_tags_json} tags_json, --replace with
    "bill_billing_period_start_date" "billing_period",
    "date_trunc"('day', "line_item_usage_start_date") "usage_date",
    "bill_payer_account_id" "payer_account_id",
    "line_item_usage_account_id" "linked_account_id",
    "line_item_resource_id" "resource_id",
    "line_item_product_code" "product_code",
    "line_item_operation" "operation",
    product['region'] "region",
    "line_item_line_item_type" "charge_type",
    "pricing_unit" "pricing_unit",
    "sum"(
        CASE
            WHEN ("line_item_line_item_type" = 'Usage') THEN "line_item_usage_amount"
            ELSE 0
        END
    ) "usage_quantity",
    "sum"("line_item_unblended_cost") "unblended_cost",
    "sum"("pricing_public_on_demand_cost") "public_cost"
FROM "${cur2_database}"."${cur2_table_name}"
WHERE (
        (
            (
                (
                    "bill_billing_period_start_date" >= (
                        "date_trunc"('month', current_timestamp) - INTERVAL '3' MONTH
                    )
                )
                AND (
                    "line_item_usage_start_date" < (
                        "date_trunc"('day', current_timestamp) - INTERVAL '1' DAY
                    )
                )
            )
            AND ("line_item_operation" LIKE '%Storage%')
        )
        AND (
            ("line_item_product_code" LIKE '%AmazonGlacier%')
            OR ("line_item_product_code" LIKE '%AmazonS3%')
        )
    )
GROUP BY 1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13