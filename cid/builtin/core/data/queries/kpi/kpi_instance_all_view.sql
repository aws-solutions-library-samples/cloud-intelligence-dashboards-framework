/*Replace customer_all in row 71 with your CUR table name */
	CREATE OR REPLACE VIEW kpi_instance_all AS
		WITH
		-- Step 1: Add instance mapping data
		instance_map AS (SELECT *
			  FROM
				kpi_instance_mapping),

		-- Step 3: Filter CUR to return all usage data
		  cur_all AS (SELECT DISTINCT
			 split_part("billing_period", '-', 1) "year"
		   , split_part("billing_period", '-', 2) "month"
		   , "bill_billing_period_start_date" "billing_period"
		   , "date_trunc"('month', "line_item_usage_start_date") "usage_date"
		   , "bill_payer_account_id" "payer_account_id"
		   , "line_item_usage_account_id" "linked_account_id"
		   , "line_item_resource_id" "resource_id"
		   , ${cur_tags_json} tags_json
		   , coalesce("line_item_line_item_type", '') "charge_type"
		   , (CASE WHEN (coalesce("savings_plan_savings_plan_a_r_n", '') <> '') THEN 'SavingsPlan' WHEN (coalesce("reservation_reservation_a_r_n", '') <> '') THEN 'Reserved' WHEN ("line_item_usage_type" LIKE '%Spot%') THEN 'Spot' ELSE 'OnDemand' END) "purchase_option"
		   , "line_item_product_code" "product_code"
		   , CASE
				WHEN ("line_item_product_code" in ('AmazonSageMaker','MachineLearningSavingsPlans')) THEN 'Machine Learning'
				WHEN ("line_item_product_code" in ('AmazonEC2','AmazonECS','AmazonEKS','AWSLambda','ComputeSavingsPlans')) THEN 'Compute'
				WHEN (("line_item_product_code" = 'AmazonElastiCache')) THEN 'ElastiCache'
				WHEN (("line_item_product_code" = 'AmazonES')) THEN	'OpenSearch'
				WHEN (("line_item_product_code" = 'AmazonRDS')) THEN 'RDS'
				WHEN (("line_item_product_code" = 'AmazonRedshift')) THEN 'Redshift'
				WHEN (("line_item_product_code" = 'AmazonDynamoDB') AND (line_item_operation = 'CommittedThroughput')) THEN 'DynamoDB'
				ELSE 'Other' END "commit_service_group"
		   , coalesce("savings_plan_offering_type", '') "savings_plan_offering_type"
		   , product['region'] "region"
		   , line_item_operation "operation"
		   , line_item_usage_type "usage_type"
		   , CASE WHEN ("line_item_product_code" in ('AmazonRDS','AmazonElastiCache')) THEN "lower"("split_part"("product_instance_type", '.', 2)) ELSE "lower"("split_part"("product_instance_type", '.', 1)) END "instance_type_family"
		   , coalesce("product_instance_type", '') "instance_type"
		   , coalesce(product['operating_system'], '') "platform"
		   , product['tenancy'] "tenancy"
		   , product['physical_processor'] "processor"
		   , (CASE
			WHEN (("line_item_line_item_type" LIKE '%Usage%') AND (product['physical_processor'] LIKE '%Graviton%')) THEN 'Graviton'
			WHEN (("line_item_line_item_type" LIKE '%Usage%') AND (product['physical_processor'] LIKE '%AMD%')) THEN 'AMD'
			WHEN line_item_product_code IN ('AmazonES','AmazonElastiCache') AND (product_instance_type LIKE '%6g%' OR product_instance_type LIKE '%7g%' OR product_instance_type LIKE '%4g%') THEN 'Graviton'
			WHEN line_item_product_code IN ('AWSLambda') AND line_item_usage_type LIKE '%ARM%' THEN 'Graviton'
			WHEN line_item_usage_type LIKE '%Fargate%' AND line_item_usage_type LIKE '%ARM%' THEN 'Graviton'
			ELSE 'Other' END) "adjusted_processor"
		   , product['database_engine'] "database_engine"
		   , product['deployment_option'] "deployment_option"
		   , product['license_model'] "license_model"
		   , product['cache_engine'] "cache_engine"
		   , "sum"("line_item_usage_amount") "usage_quantity"
		   , "sum"((CASE WHEN ("line_item_line_item_type" = 'SavingsPlanCoveredUsage') THEN ("savings_plan_savings_plan_effective_cost")
			  WHEN ("line_item_line_item_type" = 'SavingsPlanRecurringFee') THEN (("savings_plan_total_commitment_to_date" - "savings_plan_used_commitment"))
			  WHEN ("line_item_line_item_type" = 'SavingsPlanNegation') THEN 0
			  WHEN ("line_item_line_item_type" = 'SavingsPlanUpfrontFee') THEN 0
			  WHEN ("line_item_line_item_type" = 'DiscountedUsage') THEN ("reservation_effective_cost")
			  WHEN ("line_item_line_item_type" = 'RIFee') THEN (("reservation_unused_amortized_upfront_fee_for_billing_period" + "reservation_unused_recurring_fee"))
			  WHEN (("line_item_line_item_type" = 'Fee') AND (coalesce("reservation_reservation_a_r_n", '') <> '')) THEN 0 ELSE ("line_item_unblended_cost" ) END)) "amortized_cost"
		   , "sum"((CASE
				WHEN ("line_item_usage_type" LIKE '%Spot%' AND "pricing_public_on_demand_cost" > 0) THEN "pricing_public_on_demand_cost"
 				WHEN ("line_item_line_item_type" = 'SavingsPlanCoveredUsage') THEN ("pricing_public_on_demand_cost")
			  WHEN ("line_item_line_item_type" = 'SavingsPlanRecurringFee') THEN ("savings_plan_total_commitment_to_date" - "savings_plan_used_commitment")
			  WHEN ("line_item_line_item_type" = 'SavingsPlanNegation') THEN 0
			  WHEN ("line_item_line_item_type" = 'SavingsPlanUpfrontFee') THEN 0
			  WHEN ("line_item_line_item_type" = 'DiscountedUsage') THEN ("pricing_public_on_demand_cost")
			  WHEN ("line_item_line_item_type" = 'RIFee') THEN ("reservation_unused_amortized_upfront_fee_for_billing_period" + "reservation_unused_recurring_fee")
			  WHEN (("line_item_line_item_type" = 'Fee') AND (coalesce("reservation_reservation_a_r_n", '') <> '')) THEN 0 ELSE ("line_item_unblended_cost" ) END)) "adjusted_amortized_cost"
		   , "sum"("pricing_public_on_demand_cost") "public_cost"
		   FROM "${cur2_database}"."${cur2_table_name}"
		   WHERE
			(CAST("concat"("billing_period", '-01') AS date) >= ("date_trunc"('month', current_date) - INTERVAL  '3' MONTH)
		   AND ("bill_payer_account_id" <>'')
		   AND ("line_item_resource_id" <>'')
		   AND ("product_servicecode" <> 'AWSDataTransfer')
		   AND (coalesce("line_item_usage_type", '') NOT LIKE '%DataXfer%')
		   AND (("line_item_line_item_type" LIKE '%Usage%') OR ("line_item_line_item_type" = 'RIFee') OR ("line_item_line_item_type" = 'SavingsPlanRecurringFee'))
		   AND (
					(("line_item_product_code" = 'AmazonEC2') AND (coalesce("product_instance_type", '') <> '') AND ("line_item_operation" LIKE '%RunInstances%'))
				OR(("line_item_product_code" = 'AmazonElastiCache') AND (coalesce("product_instance_type", '') <> ''))
				OR (("line_item_product_code" = 'AmazonES') AND (coalesce("product_instance_type", '') <> ''))
				OR (("line_item_product_code" = 'AmazonRDS') AND (coalesce("product_instance_type", '') <> ''))
				OR (("line_item_product_code" = 'AmazonRedshift') AND (coalesce("product_instance_type", '') <> ''))
				OR (("line_item_product_code" = 'AmazonDynamoDB') AND ("line_item_operation" in ('CommittedThroughput','PayPerRequestThroughput')) AND (("line_item_usage_type" LIKE '%ReadCapacityUnit-Hrs%') or ("line_item_usage_type" LIKE '%WriteCapacityUnit-Hrs%')) AND (coalesce("line_item_usage_type", '') NOT LIKE '%Repl%'))
				OR (("line_item_product_code" = 'AWSLambda') AND ("line_item_usage_type" LIKE '%Lambda-Provisioned-GB-Second%'))
				OR (("line_item_product_code" = 'AWSLambda') AND ("line_item_usage_type" LIKE '%Lambda-GB-Second%'))
				OR (("line_item_product_code" = 'AWSLambda') AND ("line_item_usage_type" LIKE '%Lambda-Provisioned-Concurrency%'))
				OR ("line_item_usage_type" LIKE '%Fargate%')
				OR (("line_item_product_code" = 'AmazonSageMaker') AND (coalesce("product_instance_type", '') <> ''))
				OR ("line_item_product_code" = 'ComputeSavingsPlans')
				OR ("line_item_product_code" = 'MachineLearningSavingsPlans')
			))

		   GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26
		   )
		SELECT
			cur_all.*
		   , CASE
				WHEN (product_code = 'AmazonEC2' AND lower(platform) NOT LIKE '%window%') THEN latest_graviton
				WHEN (product_code = 'AmazonRDS' AND database_engine in ('Aurora MySQL','Aurora PostgreSQL','MariaDB','PostgreSQL','MySQL')) THEN latest_graviton
				WHEN (product_code = 'AmazonES') THEN latest_graviton
				WHEN (product_code = 'AmazonElastiCache') THEN latest_graviton
				END "latest_graviton"
			,	latest_amd
			, latest_intel
			, generation
			, instance_processor

/*SageMaker - Should we change sagemaker to machine learning*/
		   , CASE
				WHEN ("commit_service_group" = 'Machine Learning') THEN "adjusted_amortized_cost" ELSE 0 END "sagemaker_all_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("commit_service_group" = 'Machine Learning') AND ("instance_type" <> '')) THEN amortized_cost ELSE 0 END "sagemaker_usage_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("commit_service_group" = 'Machine Learning') AND ("instance_type" <> '') AND (purchase_option = 'OnDemand')) THEN "adjusted_amortized_cost" ELSE 0 END "sagemaker_ondemand_cost"
		   , CASE
				WHEN (("purchase_option" in ('Reserved','SavingsPlan')) AND ("commit_service_group" = 'Machine Learning')) THEN ("adjusted_amortized_cost" - "amortized_cost") ELSE 0 END "sagemaker_commit_savings"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("commit_service_group" = 'Machine Learning') AND ("instance_type" <> '') AND ("purchase_option" = 'OnDemand')) THEN ("amortized_cost" * 2E-1) ELSE 0 END "sagemaker_commit_potential_savings"  /*Uses 20% savings estimate*/
/*Compute SavingsPlan*/
		   , CASE
				WHEN ("commit_service_group" = 'Compute')  THEN "adjusted_amortized_cost" ELSE 0 END "compute_all_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("commit_service_group" = 'Compute')) THEN adjusted_amortized_cost ELSE 0 END "compute_usage_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("commit_service_group" = 'Compute') AND (purchase_option = 'OnDemand')) THEN "adjusted_amortized_cost" ELSE 0 END "compute_ondemand_cost"
		   , CASE
				WHEN (("purchase_option" in ('Reserved','SavingsPlan')) AND ("commit_service_group" = 'Compute')) THEN ("adjusted_amortized_cost" - "amortized_cost") ELSE 0 END "compute_commit_savings"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("commit_service_group" = 'Compute') AND ("purchase_option" = 'OnDemand')) THEN ("amortized_cost" * 2E-1) ELSE 0 END "compute_commit_potential_savings"  /*Uses 20% savings estimate*/

/*EC2*/
		   , CASE
				WHEN ("product_code" = 'AmazonEC2') THEN adjusted_amortized_cost ELSE 0 END ec2_all_cost
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonEC2') AND ("instance_type" <> '') AND ("operation" LIKE '%RunInstances%')) THEN amortized_cost ELSE 0 END ec2_usage_cost
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonEC2') AND ("instance_type" <> '') AND ("operation" LIKE '%RunInstances%') AND (purchase_option = 'Spot')) THEN adjusted_amortized_cost ELSE 0 END "ec2_spot_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonEC2') AND ("instance_type" <> '') AND ("operation" LIKE '%RunInstances%') AND (generation IN ('Previous')) AND (purchase_option <> 'Spot') AND (purchase_option <> 'Reserved') AND (savings_plan_offering_type NOT LIKE '%EC2%')) THEN amortized_cost ELSE 0 END "ec2_previous_generation_cost"
		   , CASE
				WHEN ("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonEC2') AND ("instance_type" <> '') AND ("operation" LIKE '%RunInstances%')
				AND (lower(platform) NOT LIKE '%window%')
				AND ((adjusted_processor = 'Graviton')
				OR (((purchase_option = 'OnDemand') OR (savings_plan_offering_type = 'ComputeSavingsPlans')) AND (adjusted_processor <> 'Graviton') AND (latest_graviton <> '')))
				 THEN amortized_cost ELSE 0 END "ec2_graviton_eligible_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonEC2') AND ("instance_type" <> '') AND ("operation" LIKE '%RunInstances%') AND (adjusted_processor = 'Graviton')) THEN amortized_cost ELSE 0 END "ec2_graviton_cost"
		   , CASE
				WHEN adjusted_processor = 'Graviton' THEN 0
				WHEN ("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonEC2') AND ("instance_type" <> '') AND ("operation" LIKE '%RunInstances%')
				AND ((adjusted_processor = 'AMD')
				OR (((purchase_option = 'OnDemand') OR (savings_plan_offering_type = 'ComputeSavingsPlans')) AND (adjusted_processor <> 'AMD') AND (latest_amd <> '')))
				THEN amortized_cost ELSE 0 END "ec2_amd_eligible_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonEC2') AND ("instance_type" <> '') AND ("operation" LIKE '%RunInstances%') AND (instance_processor = 'AMD')) THEN amortized_cost ELSE 0 END "ec2_amd_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonEC2') AND ("instance_type" <> '') AND ("operation" LIKE '%RunInstances%') AND (purchase_option <> 'Spot') AND (purchase_option <> 'Reserved') AND (savings_plan_offering_type NOT LIKE '%EC2%')) THEN (adjusted_amortized_cost * 5.5E-1) ELSE 0 END "ec2_spot_potential_savings"  /*Uses 55% savings estimate*/
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonEC2') AND ("instance_type" <> '') AND ("operation" LIKE '%RunInstances%') AND (purchase_option = 'Spot')) THEN (adjusted_amortized_cost -amortized_cost) ELSE 0 END "ec2_spot_savings"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonEC2') AND ("instance_type" <> '') AND ("operation" LIKE '%RunInstances%') AND (generation IN ('Previous')) AND (purchase_option <> 'Spot') AND (purchase_option <> 'Reserved') AND (savings_plan_offering_type NOT LIKE '%EC2%')) THEN (amortized_cost * 5E-2) ELSE 0 END "ec2_previous_generation_potential_savings"  /*Uses 5% savings estimate*/
		   , CASE
				WHEN ("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonEC2') AND ("instance_type" <> '') AND ("operation" LIKE '%RunInstances%') AND (lower(platform) NOT LIKE '%window%') AND (((purchase_option = 'OnDemand') OR (savings_plan_offering_type = 'ComputeSavingsPlans')) AND (adjusted_processor <> 'Graviton') AND (latest_graviton <> '') AND adjusted_processor <> 'AMD') THEN (amortized_cost * 2E-1)
				WHEN ("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonEC2') AND ("instance_type" <> '') AND ("operation" LIKE '%RunInstances%') AND (lower(platform) NOT LIKE '%window%') AND (((purchase_option = 'OnDemand') OR (savings_plan_offering_type = 'ComputeSavingsPlans')) AND (adjusted_processor <> 'Graviton') AND (latest_graviton <> '') AND adjusted_processor = 'AMD') THEN (amortized_cost * 1E-1) ELSE 0 END "ec2_graviton_potential_savings"  /*Uses 20% savings estimate for intel and 10% for AMD*/
		   , CASE
				WHEN ("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonEC2') AND ("instance_type" <> '') AND ("operation" LIKE '%RunInstances%') AND (((purchase_option = 'OnDemand') OR (savings_plan_offering_type = 'ComputeSavingsPlans')) AND (adjusted_processor <> 'Graviton') AND (latest_amd <> '') AND adjusted_processor <> 'AMD') THEN (amortized_cost * 1E-1) ELSE 0 END "ec2_amd_potential_savings"  /*Uses 10% savings estimate for intel and 0% for Graviton*/
/*RDS*/
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonRDS') AND ("instance_type" <> '')) THEN adjusted_amortized_cost ELSE 0 END "rds_all_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonRDS') AND ("instance_type" <> '') AND (purchase_option = 'OnDemand')) THEN adjusted_amortized_cost ELSE 0 END "rds_ondemand_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonRDS') AND (adjusted_processor = 'Graviton')) THEN amortized_cost
				WHEN (("charge_type" = 'Usage') AND ("product_code" = 'AmazonRDS') AND ("instance_type" <> '') AND (database_engine in ('Aurora MySQL','Aurora PostgreSQL','MariaDB','PostgreSQL','MySQL')) AND (adjusted_processor <> 'Graviton')  AND (latest_graviton <> '')) THEN amortized_cost ELSE 0 END "rds_graviton_eligible_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonRDS') AND ("instance_type" <> '') AND (database_engine in ('Aurora MySQL','Aurora PostgreSQL','MariaDB','PostgreSQL','MySQL')) AND (adjusted_processor = 'Graviton')) THEN amortized_cost ELSE 0 END "rds_graviton_cost"
		   , CASE
				WHEN ("charge_type" NOT LIKE '%Usage%') THEN 0
				WHEN ("product_code" <> 'AmazonRDS') THEN 0
				WHEN (adjusted_processor = 'Graviton') THEN 0
				WHEN (latest_graviton = '') THEN 0
				WHEN ((latest_graviton <> '') AND purchase_option = 'OnDemand' AND (database_engine in ('Aurora MySQL','Aurora PostgreSQL','MariaDB','PostgreSQL','MySQL'))) THEN (amortized_cost * 1E-1) ELSE 0 END "rds_graviton_potential_savings"  /*Uses 10% savings estimate*/
		   , CASE
				WHEN (("purchase_option" in ('Reserved','SavingsPlan')) AND ("product_code" = 'AmazonRDS')) THEN ("adjusted_amortized_cost" - "amortized_cost") ELSE 0 END "rds_commit_savings"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonRDS') AND ("instance_type" <> '') AND (purchase_option = 'OnDemand')) THEN (amortized_cost * 2E-1) ELSE 0 END "rds_commit_potential_savings"  /*Uses 20% savings estimate*/
			, (CASE WHEN (((("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonRDS')) AND ("instance_type" <> '')) AND (database_engine IN ('Oracle'))) THEN adjusted_amortized_cost ELSE 0 END) "rds_oracle_cost"
			, (CASE WHEN (((("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonRDS')) AND ("instance_type" <> '')) AND (database_engine IN ('SQL Server'))) THEN adjusted_amortized_cost ELSE 0 END) "rds_sql_server_cost"

/*ElastiCache*/
		   , CASE
				WHEN ("product_code" = 'AmazonElastiCache') THEN adjusted_amortized_cost ELSE 0 END "elasticache_all_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonElastiCache') AND ("instance_type" <> '')) THEN amortized_cost ELSE 0 END "elasticache_usage_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonElastiCache') AND ("instance_type" <> '') AND (purchase_option = 'OnDemand')) THEN adjusted_amortized_cost ELSE 0 END "elasticache_ondemand_cost"
		   , CASE
				WHEN (("purchase_option" in ('Reserved','SavingsPlan')) AND ("product_code" = 'AmazonElastiCache')) THEN ("adjusted_amortized_cost" - "amortized_cost") ELSE 0 END "elasticache_commit_savings"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonElastiCache') AND ("instance_type" <> '') AND (purchase_option = 'OnDemand')) THEN (amortized_cost * 2E-1) ELSE 0 END "elasticache_commit_potential_savings"  /*Uses 20% savings estimate*/
		   , CASE
				WHEN (("product_code" = 'AmazonElastiCache') AND ("instance_type" <> '') AND (adjusted_processor = 'Graviton')) THEN amortized_cost
				WHEN (("charge_type" = 'Usage') AND ("product_code" = 'AmazonElastiCache') AND ("instance_type" <> '') AND (latest_graviton <> '')) THEN amortized_cost ELSE 0 END "elasticache_graviton_eligible_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonElastiCache') AND ("instance_type" <> '') AND (instance_processor = 'Graviton')) THEN amortized_cost ELSE 0 END "elasticache_graviton_cost"
		   , CASE
				WHEN (adjusted_processor = 'Graviton') THEN 0
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonElastiCache') AND ("instance_type" <> '') AND (latest_graviton <> ''))  THEN (amortized_cost * 5E-2) ELSE 0 END "elasticache_graviton_potential_savings"  /*Uses 5% savings estimate*/
/*opensearch*/
		   , CASE
				WHEN ("product_code" = 'AmazonES') THEN adjusted_amortized_cost ELSE 0 END "opensearch_all_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonES') AND ("instance_type" <> '')) THEN amortized_cost ELSE 0 END "opensearch_usage_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonES') AND ("instance_type" <> '') AND (purchase_option = 'OnDemand')) THEN adjusted_amortized_cost ELSE 0 END "opensearch_ondemand_cost"
		   , CASE
				WHEN (("purchase_option" in ('Reserved','SavingsPlan')) AND ("product_code" = 'AmazonES')) THEN ("adjusted_amortized_cost" - "amortized_cost") ELSE 0 END "opensearch_commit_savings"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonES') AND ("instance_type" <> '') AND (purchase_option = 'OnDemand')) THEN (amortized_cost * 2E-1) ELSE 0 END "opensearch_commit_potential_savings"  /*Uses 20% savings estimate*/
		   , CASE
				WHEN (("product_code" = 'AmazonES') AND ("instance_type" <> '') AND (adjusted_processor = 'Graviton')) THEN amortized_cost
				WHEN (("charge_type" = 'Usage') AND ("product_code" = 'AmazonES') AND ("instance_type" <> '') AND (latest_graviton <> '')) THEN amortized_cost ELSE 0 END "opensearch_graviton_eligible_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonES') AND ("instance_type" <> '') AND (adjusted_processor = 'Graviton')) THEN amortized_cost ELSE 0 END "opensearch_graviton_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonES') AND ("instance_type" <> '') AND (adjusted_processor = 'Graviton')) THEN 0
				WHEN (("charge_type" = 'Usage') AND ("product_code" = 'AmazonES') AND ("instance_type" <> '') AND (latest_graviton <> '')) THEN (amortized_cost * 5E-2)
				ELSE 0 END "opensearch_graviton_potential_savings"  /*Uses 5% savings estimate*/
/*Redshift*/
		   , CASE
				WHEN ("product_code" = 'AmazonRedshift') THEN adjusted_amortized_cost ELSE 0 END "redshift_all_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonRedshift') AND ("instance_type" <> '')) THEN amortized_cost ELSE 0 END "redshift_usage_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonRedshift') AND ("instance_type" <> '') AND (purchase_option = 'OnDemand')) THEN adjusted_amortized_cost ELSE 0 END "redshift_ondemand_cost"
		   , CASE
				WHEN (("purchase_option" in ('Reserved','SavingsPlan')) AND ("product_code" = 'AmazonRedshift')) THEN ("adjusted_amortized_cost" - "amortized_cost") ELSE 0 END "redshift_commit_savings"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonRedshift') AND ("instance_type" <> '') AND (purchase_option = 'OnDemand')) THEN (amortized_cost * 2E-1) ELSE 0 END "redshift_commit_potential_savings"  /*Uses 20% savings estimate*/
/*DynamoDB*/
		   , CASE
				WHEN ("product_code" = 'AmazonDynamoDB') THEN "adjusted_amortized_cost" ELSE 0 END "dynamodb_all_cost"
		   , CASE
				WHEN ("charge_type" LIKE '%Usage%') AND ("commit_service_group" = 'DynamoDB') THEN "adjusted_amortized_cost" ELSE 0 END "dynamodb_committed_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AmazonDynamoDB')) THEN amortized_cost ELSE 0 END "dynamodb_usage_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND  ("commit_service_group" = 'DynamoDB') AND ("purchase_option" = 'OnDemand')) THEN "adjusted_amortized_cost" ELSE 0 END "dynamodb_ondemand_cost"
		   , CASE
				WHEN (("purchase_option" in ('Reserved','SavingsPlan')) AND ("commit_service_group" = 'DynamoDB')) THEN ("adjusted_amortized_cost" - "amortized_cost") ELSE 0 END "dynamodb_commit_savings"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND  ("commit_service_group" = 'DynamoDB') AND (purchase_option = 'OnDemand')) THEN (amortized_cost * 2E-1) ELSE 0 END "dynamodb_commit_potential_savings"  /*Uses 20% savings estimate*/
/*Lambda*/
		   , CASE
				WHEN ("product_code" = 'AWSLambda') THEN "adjusted_amortized_cost" ELSE 0 END "lambda_all_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AWSLambda')) THEN amortized_cost ELSE 0 END "lambda_usage_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND("product_code" = 'AWSLambda') AND (adjusted_processor = 'Graviton')) THEN amortized_cost
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AWSLambda')) THEN amortized_cost ELSE 0 END "lambda_graviton_eligible_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AWSLambda') AND (adjusted_processor = 'Graviton')) THEN amortized_cost ELSE 0 END "lambda_graviton_cost"
		   , CASE
				WHEN (("charge_type" LIKE '%Usage%') AND ("product_code" = 'AWSLambda') AND (adjusted_processor <> 'Graviton')) THEN amortized_cost*.2 ELSE 0 END "lambda_graviton_potential_savings"  /*Uses 20% savings estimate*/

		FROM
			cur_all cur_all
			LEFT JOIN instance_map ON (instance_map.product = product_code AND instance_map.family = instance_type_family)

