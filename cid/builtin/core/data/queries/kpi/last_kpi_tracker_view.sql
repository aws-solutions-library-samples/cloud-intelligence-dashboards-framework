CREATE OR REPLACE VIEW kpi_tracker AS
SELECT DISTINCT
  spend_all.billing_period
, spend_all.payer_account_id
, spend_all.linked_account_id
, spend_all.spend_all_cost
, spend_all.tags_json
, instance_all.ec2_all_cost
, instance_all.ec2_usage_cost
, instance_all.ec2_spot_cost
, instance_all.ec2_spot_potential_savings
, instance_all.ec2_previous_generation_cost
, instance_all.ec2_previous_generation_potential_savings
, instance_all.ec2_graviton_eligible_cost
, instance_all.ec2_graviton_cost
, instance_all.ec2_graviton_potential_savings
, instance_all.ec2_amd_eligible_cost
, instance_all.ec2_amd_cost
, instance_all.ec2_amd_potential_savings
, instance_all.rds_all_cost
, instance_all.rds_ondemand_cost
, instance_all.rds_graviton_cost
, instance_all.rds_graviton_eligible_cost
, instance_all.rds_graviton_potential_savings
, instance_all.rds_commit_potential_savings
, instance_all.rds_commit_savings
, instance_all.elasticache_all_cost
, instance_all.elasticache_ondemand_cost
, instance_all.elasticache_graviton_cost
, instance_all.elasticache_graviton_eligible_cost
, instance_all.elasticache_graviton_potential_savings
, instance_all.elasticache_commit_potential_savings
, instance_all.elasticache_commit_savings
, ebs_all.ebs_all_cost
, ebs_all.ebs_gp_all_cost
, ebs_all.ebs_gp2_cost
, ebs_all.ebs_gp3_cost
, ebs_all.ebs_gp3_potential_savings
, snap.ebs_snapshots_under_1yr_cost
, snap.ebs_snapshots_over_1yr_cost
, snap.ebs_snapshot_cost
, s3_all.s3_all_storage_cost
, s3_all.s3_standard_storage_cost
, s3_all.s3_standard_storage_potential_savings
, instance_all.compute_all_cost
, instance_all.compute_ondemand_cost
, instance_all.compute_commit_potential_savings
, instance_all.compute_commit_savings
, instance_all.dynamodb_all_cost
, instance_all.dynamodb_committed_cost
, instance_all.dynamodb_ondemand_cost
, instance_all.dynamodb_commit_potential_savings
, instance_all.dynamodb_commit_savings
, instance_all.opensearch_all_cost
, instance_all.opensearch_ondemand_cost
, instance_all.opensearch_graviton_cost
, instance_all.opensearch_graviton_eligible_cost
, instance_all.opensearch_graviton_potential_savings
, instance_all.opensearch_commit_potential_savings
, instance_all.opensearch_commit_savings
, instance_all.redshift_all_cost
, instance_all.redshift_ondemand_cost
, instance_all.redshift_commit_potential_savings
, instance_all.redshift_commit_savings
, instance_all.sagemaker_all_cost
, instance_all.sagemaker_ondemand_cost
, instance_all.sagemaker_commit_potential_savings
, instance_all.sagemaker_commit_savings
, instance_all.lambda_all_cost
, instance_all.lambda_graviton_cost
, instance_all.lambda_graviton_eligible_cost
, instance_all.lambda_graviton_potential_savings
, instance_all.rds_license
, instance_all.rds_no_license
, instance_all.rds_sql_server_cost
, instance_all.rds_oracle_cost
FROM
  (
   SELECT DISTINCT
     billing_period
   , payer_account_id
   , linked_account_id
   , tags_json
   , "sum"(amortized_cost) "spend_all_cost"
   , "sum"(unblended_cost) "unblended_cost"
   FROM
     summary_view
   WHERE (CAST("concat"("year", '-', "month", '-01') AS date) >= ("date_trunc"('month', current_date) - INTERVAL  '3' MONTH))
   GROUP BY 1, 2, 3, 4
) spend_all
LEFT JOIN (
   SELECT DISTINCT
     billing_period
   , payer_account_id
   , linked_account_id
   , tags_json
   , "sum"("ec2_all_cost") "ec2_all_cost"
   , "sum"("ec2_usage_cost") "ec2_usage_cost"
   , "sum"("ec2_spot_cost") "ec2_spot_cost"
   , "sum"("ec2_spot_potential_savings") "ec2_spot_potential_savings"
   , "sum"("ec2_previous_generation_cost") "ec2_previous_generation_cost"
   , "sum"("ec2_previous_generation_potential_savings") "ec2_previous_generation_potential_savings"
   , "sum"("ec2_graviton_eligible_cost") "ec2_graviton_eligible_cost"
   , "sum"("ec2_graviton_cost") "ec2_graviton_cost"
   , "sum"("ec2_graviton_potential_savings") "ec2_graviton_potential_savings"
   , "sum"("ec2_amd_eligible_cost") "ec2_amd_eligible_cost"
   , "sum"("ec2_amd_cost") "ec2_amd_cost"
   , "sum"("ec2_amd_potential_savings") "ec2_amd_potential_savings"
   , "sum"("rds_all_cost") "rds_all_cost"
   , "sum"("rds_ondemand_cost") "rds_ondemand_cost"
   , "sum"("rds_graviton_cost") "rds_graviton_cost"
   , "sum"("rds_graviton_eligible_cost") "rds_graviton_eligible_cost"
   , "sum"("rds_graviton_potential_savings") "rds_graviton_potential_savings"
   , "sum"("rds_commit_potential_savings") "rds_commit_potential_savings"
   , "sum"("rds_commit_savings") "rds_commit_savings"
   , "sum"((CASE WHEN ("license_model" IN ('License included', 'Bring your own license')) THEN 1 ELSE 0 END)) "rds_license"
   , "sum"((CASE WHEN ("license_model" LIKE 'No license required') THEN 1 ELSE 0 END)) "rds_no_license"
   , "sum"("elasticache_all_cost") "elasticache_all_cost"
   , "sum"("elasticache_ondemand_cost") "elasticache_ondemand_cost"
   , "sum"("elasticache_graviton_cost") "elasticache_graviton_cost"
   , "sum"("elasticache_graviton_eligible_cost") "elasticache_graviton_eligible_cost"
   , "sum"("elasticache_graviton_potential_savings") "elasticache_graviton_potential_savings"
   , "sum"("elasticache_commit_potential_savings") "elasticache_commit_potential_savings"
   , "sum"("elasticache_commit_savings") "elasticache_commit_savings"
   , "sum"("compute_all_cost") "compute_all_cost"
   , "sum"("compute_ondemand_cost") "compute_ondemand_cost"
   , "sum"("compute_commit_potential_savings") "compute_commit_potential_savings"
   , "sum"("compute_commit_savings") "compute_commit_savings"
   , "sum"("opensearch_all_cost") "opensearch_all_cost"
   , "sum"("opensearch_ondemand_cost") "opensearch_ondemand_cost"
   , "sum"("opensearch_graviton_cost") "opensearch_graviton_cost"
   , "sum"("opensearch_graviton_eligible_cost") "opensearch_graviton_eligible_cost"
   , "sum"("opensearch_graviton_potential_savings") "opensearch_graviton_potential_savings"
   , "sum"("opensearch_commit_potential_savings") "opensearch_commit_potential_savings"
   , "sum"("opensearch_commit_savings") "opensearch_commit_savings"
   , "sum"("redshift_all_cost") "redshift_all_cost"
   , "sum"("redshift_ondemand_cost") "redshift_ondemand_cost"
   , "sum"("redshift_commit_potential_savings") "redshift_commit_potential_savings"
   , "sum"("redshift_commit_savings") "redshift_commit_savings"
   , "sum"("dynamodb_all_cost") "dynamodb_all_cost"
   , "sum"("dynamodb_committed_cost") "dynamodb_committed_cost"
   , "sum"("dynamodb_ondemand_cost") "dynamodb_ondemand_cost"
   , "sum"("dynamodb_commit_potential_savings") "dynamodb_commit_potential_savings"
   , "sum"("dynamodb_commit_savings") "dynamodb_commit_savings"
   , "sum"("sagemaker_all_cost") "sagemaker_all_cost"
   , "sum"("sagemaker_ondemand_cost") "sagemaker_ondemand_cost"
   , "sum"("sagemaker_commit_potential_savings") "sagemaker_commit_potential_savings"
   , "sum"("sagemaker_commit_savings") "sagemaker_commit_savings"
   , "sum"("lambda_all_cost") "lambda_all_cost"
   , "sum"("lambda_graviton_cost") "lambda_graviton_cost"
   , "sum"("lambda_graviton_eligible_cost") "lambda_graviton_eligible_cost"
   , "sum"("lambda_graviton_potential_savings") "lambda_graviton_potential_savings"
   , "sum"("rds_sql_server_cost") "rds_sql_server_cost"
   , "sum"("rds_oracle_cost") "rds_oracle_cost"
   FROM
     kpi_instance_all
   GROUP BY 1, 2, 3, 4
)  instance_all ON ((instance_all.linked_account_id = spend_all.linked_account_id) AND (instance_all.billing_period = spend_all.billing_period) AND (instance_all.payer_account_id = spend_all.payer_account_id) AND (instance_all.tags_json = spend_all.tags_json))
LEFT JOIN (
   SELECT DISTINCT
     billing_period
   , payer_account_id
   , linked_account_id
   , tags_json
   , "sum"("ebs_all_cost") "ebs_all_cost"
   , "sum"("ebs_gp3_cost"+"ebs_gp2_cost") "ebs_gp_all_cost"
   , "sum"("ebs_gp3_cost") "ebs_gp3_cost"
   , "sum"("ebs_gp2_cost") "ebs_gp2_cost"
   , "sum"("ebs_gp3_potential_savings") "ebs_gp3_potential_savings"
   FROM
     kpi_ebs_storage_all
   GROUP BY 1, 2, 3, 4
)  ebs_all ON ((ebs_all.linked_account_id = spend_all.linked_account_id) AND (ebs_all.billing_period = spend_all.billing_period) AND (ebs_all.payer_account_id = spend_all.payer_account_id) AND (ebs_all.tags_json = spend_all.tags_json) )
LEFT JOIN (
   SELECT DISTINCT
     billing_period
   , payer_account_id
   , linked_account_id
   , tags_json
   , "sum"("ebs_snapshots_under_1yr_cost") "ebs_snapshots_under_1yr_cost"
   , "sum"("ebs_snapshots_over_1yr_cost") "ebs_snapshots_over_1yr_cost"
   , "sum"("ebs_snapshot_cost") "ebs_snapshot_cost"
   FROM
     kpi_ebs_snap
   GROUP BY 1, 2, 3, 4
)  snap ON ((snap.linked_account_id = spend_all.linked_account_id) AND (snap.billing_period = spend_all.billing_period) AND (snap.payer_account_id = spend_all.payer_account_id)  AND (snap.tags_json = spend_all.tags_json))
LEFT JOIN (
   SELECT DISTINCT
     billing_period
   , payer_account_id
   , linked_account_id
   , tags_json
   , "sum"("s3_all_storage_cost") "s3_all_storage_cost"
   , "sum"("s3_standard_storage_cost") "s3_standard_storage_cost"
   , "sum"("s3_standard_storage_potential_savings") "s3_standard_storage_potential_savings"
   FROM
     kpi_s3_storage_all
   GROUP BY 1, 2, 3, 4
)  s3_all ON ((s3_all.linked_account_id = spend_all.linked_account_id) AND (s3_all.billing_period = spend_all.billing_period) AND (s3_all.payer_account_id = spend_all.payer_account_id) AND (s3_all.tags_json = spend_all.tags_json))
WHERE (spend_all.billing_period >= ("date_trunc"('month', current_timestamp) - INTERVAL  '3' MONTH))