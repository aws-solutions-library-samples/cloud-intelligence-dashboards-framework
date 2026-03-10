#!/bin/bats

account_id=$(aws sts get-caller-identity --query "Account" --output text )
quicksight_group="${quicksight_group:-cid-owners}" # If variable not set or null, use default
quicksight_datasource_id="${quicksight_datasource_id:-CID-CMD-Athena}" # If variable not set or null, use default

@test "Install" {
  run cid-cmd -vv deploy  \
    --dashboard-id compute-optimizer-dashboard \
    --share-with-account \
    --athena-database 'optimization_data' \
    --athena-workgroup primary\
    --quicksight-group $quicksight_group \
    --quicksight-datasource-id $quicksight_datasource_id \
    --view-compute-optimizer-lambda-lines-s3FolderPath       's3://cid-data-{account_id}/compute_optimizer/compute_optimizer_ec2_lambda' \
    --view-compute-optimizer-ebs-volume-lines-s3FolderPath   's3://cid-data-{account_id}/compute_optimizer/compute_optimizer_ebs_volume' \
    --view-compute-optimizer-auto-scale-lines-s3FolderPath   's3://cid-data-{account_id}/compute_optimizer/compute_optimizer_auto_scale' \
    --view-compute-optimizer-ec2-instance-lines-s3FolderPath 's3://cid-data-{account_id}/compute_optimizer/compute_optimizer_ec2_instance'

  [ "$status" -eq 0 ]
}

@test "Views created" {
  run aws athena get-table-metadata \
    --catalog-name 'AwsDataCatalog'\
    --database-name 'optimization_data' \
    --table-name 'compute_optimizer_all_options'

  [ "$status" -eq 0 ]
}

@test "Dataset created" {
  run aws quicksight describe-data-set \
    --aws-account-id $account_id \
    --data-set-id compute_optimizer_all_options

  [ "$status" -eq 0 ]
}

@test "Dashboard created" {
  run aws quicksight describe-dashboard \
    --aws-account-id $account_id \
    --dashboard-id compute-optimizer-dashboard

  [ "$status" -eq 0 ]
}

@test "Update works" {
  run cid-cmd -vv --yes update --force --recursive  \
    --dashboard-id compute-optimizer-dashboard \
    --athena-workgroup primary\
    --quicksight-group $quicksight_group \
    --quicksight-datasource-id $quicksight_datasource_id \
    --view-compute-optimizer-lambda-lines-s3FolderPath       's3://cid-data-{account_id}/compute_optimizer/compute_optimizer_ec2_lambda' \
    --view-compute-optimizer-ebs-volume-lines-s3FolderPath   's3://cid-data-{account_id}/compute_optimizer/compute_optimizer_ebs_volume' \
    --view-compute-optimizer-auto-scale-lines-s3FolderPath   's3://cid-data-{account_id}/compute_optimizer/compute_optimizer_auto_scale' \
    --view-compute-optimizer-ec2-instance-lines-s3FolderPath 's3://cid-data-{account_id}/compute_optimizer/compute_optimizer_ec2_instance'

  [ "$status" -eq 0 ]
  echo "$output" | grep 'Update completed'
}

@test "Delete runs" {
  run cid-cmd -vv --yes delete \
    --dashboard-id compute-optimizer-dashboard

  [ "$status" -eq 0 ]
}

@test "Dashboard is deleted" {
  run aws quicksight describe-dashboard \
    --aws-account-id $account_id \
    --dashboard-id compute-optimizer-dashboard

  [ "$status" -ne 0 ]
}

@test "Dataset is deleted" {
  run aws quicksight describe-data-set \
    --aws-account-id $account_id \
    --data-set-id compute_optimizer_all_options

  [ "$status" -ne 0 ]
}

@test "View is deleted" {
  run aws athena get-table-metadata \
    --catalog-name 'AwsDataCatalog'\
    --database-name 'optimization_data' \
    --table-name 'compute_optimizer_all_options'

  [ "$status" -ne 0 ]
}