resource "aws_cloudformation_stack" "cid_dataexports_destination" {
  # checkov:skip=CKV_AWS_124:SNS topic not required for this use case
  provider     = aws.destination_account
  name         = "CID-DataExports-Destination"
  template_url = local.template_urls.data_exports
  capabilities = local.common_capabilities

  parameters = {
    DestinationAccountId = var.global_values.destination_account_id
    ResourcePrefix       = var.cid_dataexports_destination.resource_prefix
    ManageCUR2           = var.cid_dataexports_destination.manage_cur2
    ManageFOCUS          = var.cid_dataexports_destination.manage_focus
    ManageCOH            = var.cid_dataexports_destination.manage_coh
    SourceAccountIds     = var.global_values.source_account_ids
    EnableSCAD           = var.cid_dataexports_destination.enable_scad
    RolePath             = var.cid_dataexports_destination.role_path
    TimeGranularity      = var.cid_dataexports_destination.time_granularity
  }

  timeouts {
    create = lookup(local.default_timeouts, "create", "30m")
    update = lookup(local.default_timeouts, "update", "30m")
    delete = lookup(local.default_timeouts, "delete", "30m")
  }

  lifecycle {
    create_before_destroy = true
  }
}


resource "aws_cloudformation_stack" "cid_dataexports_source" {
  # checkov:skip=CKV_AWS_124:SNS topic not required for this use case
  depends_on = [
    aws_cloudformation_stack.cid_dataexports_destination
  ]
  name         = "CID-DataExports-Source"
  provider     = aws
  template_url = local.template_urls.data_exports
  capabilities = local.common_capabilities

  parameters = {
    DestinationAccountId = var.global_values.destination_account_id
    ResourcePrefix       = var.cid_dataexports_source.source_resource_prefix
    ManageCUR2           = var.cid_dataexports_source.source_manage_cur2
    ManageFOCUS          = var.cid_dataexports_source.source_manage_focus
    ManageCOH            = var.cid_dataexports_source.source_manage_coh
    SourceAccountIds     = var.global_values.source_account_ids
    EnableSCAD           = var.cid_dataexports_source.source_enable_scad
    RolePath             = var.cid_dataexports_source.source_role_path
    TimeGranularity      = var.cid_dataexports_source.source_time_granularity
  }

  timeouts {
    create = lookup(local.default_timeouts, "create", "30m")
    update = lookup(local.default_timeouts, "update", "30m")
    delete = lookup(local.default_timeouts, "delete", "30m")
  }

  lifecycle {
    create_before_destroy = true
  }
}


resource "aws_cloudformation_stack" "cloud_intelligence_dashboards" {
  # checkov:skip=CKV_AWS_124:SNS topic not required for this use case
  name     = "Cloud-Intelligence-Dashboards"
  provider = aws.destination_account
  depends_on = [
    aws_cloudformation_stack.cid_dataexports_source,
    aws_cloudformation_stack.cid_dataexports_destination
  ]
  capabilities = local.common_capabilities
  template_url = local.template_urls.cudos

  parameters = {
    # Prerequisites Variables
    PrerequisitesQuickSight            = var.cloud_intelligence_dashboards.prerequisites_quicksight
    PrerequisitesQuickSightPermissions = var.cloud_intelligence_dashboards.prerequisites_quicksight_permissions
    QuickSightUser                     = var.global_values.quicksight_user
    LakeFormationEnabled               = var.cloud_intelligence_dashboards.lake_formation_enabled

    # CUR Parameters
    CURVersion                      = var.cloud_intelligence_dashboards.cur_version
    DeployCUDOSv5                   = var.cloud_intelligence_dashboards.deploy_cudos_v5
    DeployCostIntelligenceDashboard = var.cloud_intelligence_dashboards.deploy_cost_intelligence_dashboard
    DeployKPIDashboard              = var.cloud_intelligence_dashboards.deploy_kpi_dashboard

    # Optimization Parameters
    OptimizationDataCollectionBucketPath = var.cloud_intelligence_dashboards.optimization_data_collection_bucket_path
    DeployTAODashboard                   = var.cloud_intelligence_dashboards.deploy_tao_dashboard
    DeployComputeOptimizerDashboard      = var.cloud_intelligence_dashboards.deploy_compute_optimizer_dashboard
    PrimaryTagName                       = var.cloud_intelligence_dashboards.primary_tag_name
    SecondaryTagName                     = var.cloud_intelligence_dashboards.secondary_tag_name

    # Technical Parameters
    AthenaWorkgroup                  = var.cloud_intelligence_dashboards.athena_workgroup
    AthenaQueryResultsBucket         = var.cloud_intelligence_dashboards.athena_query_results_bucket
    DatabaseName                     = var.cloud_intelligence_dashboards.database_name
    GlueDataCatalog                  = var.cloud_intelligence_dashboards.glue_data_catalog
    Suffix                           = var.cloud_intelligence_dashboards.suffix
    QuickSightDataSourceRoleName     = var.cloud_intelligence_dashboards.quicksight_data_source_role_name
    QuickSightDataSetRefreshSchedule = var.cloud_intelligence_dashboards.quicksight_data_set_refresh_schedule
    LambdaLayerBucketPrefix          = var.cloud_intelligence_dashboards.lambda_layer_bucket_prefix
    DeployCUDOSDashboard             = var.cloud_intelligence_dashboards.deploy_cudos_dashboard
    DataBucketsKmsKeysArns           = var.cloud_intelligence_dashboards.data_buckets_kms_keys_arns
    DeploymentType                   = var.cloud_intelligence_dashboards.deployment_type
    ShareDashboard                   = var.cloud_intelligence_dashboards.share_dashboard

    # Legacy Parameters
    KeepLegacyCURTable  = var.cloud_intelligence_dashboards.keep_legacy_cur_table
    CURBucketPath       = var.cloud_intelligence_dashboards.cur_bucket_path
    CURTableName        = var.cloud_intelligence_dashboards.cur_table_name
    PermissionsBoundary = var.cloud_intelligence_dashboards.permissions_boundary
    RolePath            = var.cloud_intelligence_dashboards.role_path
  }

  timeouts {
    create = "60m"
    update = "60m"
    delete = "60m"
  }

  lifecycle {
    create_before_destroy = true
    ignore_changes = [
      # Ignore changes to tags
      tags
    ]
  }
}