/*Replace customer_all in row 22 with your CUR table name */
CREATE OR REPLACE VIEW kpi_ebs_storage_all AS
WITH
-- Step 1: Filter CUR to return all EC2 EBS storage usage data
ebs_all AS (
	SELECT
	bill_billing_period_start_date
	, line_item_usage_start_date
	, bill_payer_account_id
	, line_item_usage_account_id
	, ${cur_tags_json} tags_json
	, line_item_resource_id
	, product['volume_api_name'] product_volume_api_name
	, line_item_usage_type
	, pricing_unit
	, line_item_unblended_cost
	, line_item_usage_amount
	FROM
		"${cur2_database}"."${cur2_table_name}"
	WHERE (line_item_product_code = 'AmazonEC2') AND (line_item_line_item_type = 'Usage')
	AND coalesce(bill_payer_account_id, '') <> ''
	AND coalesce(line_item_usage_account_id, '') <> ''
	AND (CAST("concat"("billing_period", '-01') AS date) >= ("date_trunc"('month', current_date) - INTERVAL  '3' MONTH))
	AND coalesce(product['volume_api_name'], '') <> ''
	AND coalesce(line_item_usage_type, '') NOT LIKE '%Snap%'
	AND line_item_usage_type LIKE '%EBS%'
),

-- Step 2: Pivot table so storage types cost and usage into separate columns
ebs_spend AS (
	SELECT DISTINCT
	bill_billing_period_start_date AS billing_period
	, date_trunc('month',line_item_usage_start_date) AS usage_date
	, bill_payer_account_id AS payer_account_id
	, line_item_usage_account_id AS linked_account_id
	, tags_json
	, line_item_resource_id AS resource_id
	, coalesce(product_volume_api_name, '') AS volume_api_name
	, SUM (CASE
			WHEN (((pricing_unit = 'GB-Mo' or pricing_unit = 'GB-month') or pricing_unit = 'GB-month') AND  line_item_usage_type LIKE '%EBS:VolumeUsage%')
			THEN  line_item_usage_amount ELSE 0 END) "usage_storage_gb_mo"
	, SUM (CASE
		WHEN (pricing_unit = 'IOPS-Mo' AND  line_item_usage_type LIKE '%IOPS%') THEN line_item_usage_amount
		ELSE 0 END) "usage_iops_mo"
	, SUM (CASE WHEN (pricing_unit = 'GiBps-mo' AND  line_item_usage_type LIKE '%Throughput%') THEN  line_item_usage_amount ELSE 0 END) "usage_throughput_gibps_mo"
	, SUM (CASE WHEN ((pricing_unit = 'GB-Mo' or pricing_unit = 'GB-month') AND  line_item_usage_type LIKE '%EBS:VolumeUsage%') THEN  (line_item_unblended_cost) ELSE 0 END) "cost_storage_gb_mo"
	, SUM (CASE WHEN (pricing_unit = 'IOPS-Mo' AND  line_item_usage_type LIKE '%IOPS%') THEN  (line_item_unblended_cost) ELSE 0 END) "cost_iops_mo"
	, SUM (CASE WHEN (pricing_unit = 'GiBps-mo' AND  line_item_usage_type LIKE '%Throughput%') THEN  (line_item_unblended_cost) ELSE 0 END) "cost_throughput_gibps_mo"
	FROM
		ebs_all
	GROUP BY 1, 2, 3, 4, 5, 6, 7
),

ebs_spend_with_unit_cost AS (
	SELECT
	*
	, cost_storage_gb_mo/usage_storage_gb_mo AS "current_unit_cost"
	, CASE
		WHEN usage_storage_gb_mo <= 150 THEN 'under 150GB-Mo'
		WHEN usage_storage_gb_mo > 150 AND usage_storage_gb_mo <= 1000 THEN 'between 150-1000GB-Mo'
		ELSE 'over 1000GB-Mo'
	END AS storage_summary
	, CASE
		WHEN volume_api_name <> 'gp2' THEN 0
		WHEN usage_storage_gb_mo*3 < 3000 THEN 3000 - 3000
		WHEN usage_storage_gb_mo*3 > 16000 THEN 16000 - 3000
		ELSE usage_storage_gb_mo*3 - 3000
	END AS gp2_usage_added_iops_mo
	, CASE
		WHEN volume_api_name <> 'gp2' THEN 0
		WHEN usage_storage_gb_mo <= 150 THEN 0
		ELSE 125
	END AS gp2_usage_added_throughput_gibps_mo
	, cost_storage_gb_mo + cost_iops_mo + cost_throughput_gibps_mo AS ebs_all_cost
	, CASE
		WHEN volume_api_name = 'sc1' THEN  (cost_iops_mo + cost_throughput_gibps_mo + cost_storage_gb_mo)
		ELSE 0
	END "ebs_sc1_cost"
	, CASE
		WHEN volume_api_name = 'st1' THEN  (cost_iops_mo + cost_throughput_gibps_mo + cost_storage_gb_mo)
		ELSE 0
	END "ebs_st1_cost"
	, CASE
		WHEN volume_api_name = 'standard' THEN  (cost_iops_mo + cost_throughput_gibps_mo + cost_storage_gb_mo)
		ELSE 0
	END "ebs_standard_cost"
	, CASE
		WHEN volume_api_name = 'io1' THEN  (cost_iops_mo + cost_throughput_gibps_mo + cost_storage_gb_mo)
		ELSE 0
	END "ebs_io1_cost"
	, CASE
		WHEN volume_api_name = 'io2' THEN  (cost_iops_mo + cost_throughput_gibps_mo + cost_storage_gb_mo)
		ELSE 0
	END "ebs_io2_cost"
	, CASE
		WHEN volume_api_name = 'gp2' THEN  (cost_iops_mo + cost_throughput_gibps_mo + cost_storage_gb_mo)
		ELSE 0
	END "ebs_gp2_cost"
	, CASE
		WHEN volume_api_name = 'gp3' THEN  (cost_iops_mo + cost_throughput_gibps_mo + cost_storage_gb_mo)
		ELSE 0
	END "ebs_gp3_cost"
	, CASE
		WHEN volume_api_name = 'gp2' THEN cost_storage_gb_mo*0.8/usage_storage_gb_mo
		ELSE 0
	END AS "estimated_gp3_unit_cost"
	FROM
		ebs_spend
)
	SELECT DISTINCT
	billing_period
	, payer_account_id
	, linked_account_id
	, tags_json
	, resource_id
	, volume_api_name
	, storage_summary
	, sum(usage_storage_gb_mo) AS usage_storage_gb_mo
	, sum(usage_iops_mo) AS usage_iops_mo
	, sum(usage_throughput_gibps_mo) AS usage_throughput_gibps_mo
	, sum(gp2_usage_added_iops_mo) gp2_usage_added_iops_mo
	, sum(gp2_usage_added_throughput_gibps_mo) AS gp2_usage_added_throughput_gibps_mo
	, sum(ebs_all_cost) AS ebs_all_cost
	, sum(ebs_sc1_cost) AS ebs_sc1_cost
	, sum(ebs_st1_cost) AS ebs_st1_cost
	, sum(ebs_standard_cost) AS ebs_standard_cost
	, sum(ebs_io1_cost) AS ebs_io1_cost
	, sum(ebs_io2_cost) AS ebs_io2_cost
	, sum(ebs_gp2_cost) AS ebs_gp2_cost
	, sum(ebs_gp3_cost) AS ebs_gp3_cost

/* Calculate cost for gp2 gp3 estimate using the following
		- Storage always 20% cheaper
		- Additional iops per iops-mo is 6% of the cost of 1 gp3 GB-mo
		- Additional throughput per gibps-mo is 50% of the cost of 1 gp3 GB-mo */
, sum(CASE
		/*ignore non gp2' */
		WHEN volume_api_name = 'gp2' THEN ebs_gp2_cost
			- (cost_storage_gb_mo*0.8
				+ estimated_gp3_unit_cost * 0.5 * gp2_usage_added_throughput_gibps_mo
				+ estimated_gp3_unit_cost * 0.06 * gp2_usage_added_iops_mo)
		ELSE 0
		END) AS ebs_gp3_potential_savings
FROM
	ebs_spend_with_unit_cost
GROUP BY 1, 2, 3, 4, 5, 6, 7