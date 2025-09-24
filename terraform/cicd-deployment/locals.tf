locals {
  # Get destination_role_arn from TF_VAR environment variable
  destination_role_arn = var.destination_role_arn

  # # Create an effective global_values with the potentially overridden destination_role_arn
  # effective_global_values = merge(var.global_values, {
  #   destination_role_arn = local.destination_role_arn != "" ? local.destination_role_arn : var.global_values.destination_role_arn
  # })

  # Common CloudFormation template parameters
  common_template_url_base = "https://aws-managed-cost-intelligence-dashboards.s3.amazonaws.com/cfn"

  # Common CloudFormation capabilities
  common_capabilities = ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"]

  # Template validation
  template_urls = {
    data_exports = "${local.common_template_url_base}/data-exports/${var.global_values.data_export_version}/data-exports-aggregation.yaml"
    cudos        = "${local.common_template_url_base}/${var.global_values.cid_cfn_version}/cid-cfn.yml"
    cid_plugin   = "${local.common_template_url_base}/${var.global_values.cid_cfn_version}/cid-plugin.yml"
  }

  # Foundational stacks configuration
  foundational_stacks = {
    cid_dataexports_destination = {
      enabled      = true
      stack_name   = "CID-DataExports-Destination"
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
      timeouts = local.default_timeouts
      tags     = local.common_tags
    }
    cid_dataexports_source = {
      enabled      = var.global_values.source_account_ids != var.global_values.destination_account_id
      stack_name   = "CID-DataExports-Source"
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
      timeouts = local.default_timeouts
      tags     = local.common_tags
    }
    cloud_intelligence_dashboards = {
      enabled      = true
      stack_name   = "Cloud-Intelligence-Dashboards"
      template_url = local.template_urls.cudos
      capabilities = local.common_capabilities
      parameters = {
        PrerequisitesQuickSight              = var.cloud_intelligence_dashboards.prerequisites_quicksight
        PrerequisitesQuickSightPermissions   = var.cloud_intelligence_dashboards.prerequisites_quicksight_permissions
        QuickSightUser                       = var.global_values.quicksight_user
        LakeFormationEnabled                 = var.cloud_intelligence_dashboards.lake_formation_enabled
        CURVersion                           = var.cloud_intelligence_dashboards.cur_version
        DeployCUDOSv5                        = var.dashboards.cudos_v5
        DeployCostIntelligenceDashboard      = var.dashboards.cost_intelligence
        DeployKPIDashboard                   = var.dashboards.kpi
        OptimizationDataCollectionBucketPath = var.cloud_intelligence_dashboards.optimization_data_collection_bucket_path
        DeployTAODashboard                   = var.cloud_intelligence_dashboards.deploy_tao_dashboard
        DeployComputeOptimizerDashboard      = var.cloud_intelligence_dashboards.deploy_compute_optimizer_dashboard
        PrimaryTagName                       = var.cloud_intelligence_dashboards.primary_tag_name
        SecondaryTagName                     = var.cloud_intelligence_dashboards.secondary_tag_name
        AthenaWorkgroup                      = var.cloud_intelligence_dashboards.athena_workgroup
        AthenaQueryResultsBucket             = var.cloud_intelligence_dashboards.athena_query_results_bucket
        DatabaseName                         = var.cloud_intelligence_dashboards.database_name
        GlueDataCatalog                      = var.cloud_intelligence_dashboards.glue_data_catalog
        Suffix                               = var.cloud_intelligence_dashboards.suffix
        QuickSightDataSourceRoleName         = var.cloud_intelligence_dashboards.quicksight_data_source_role_name
        QuickSightDataSetRefreshSchedule     = var.cloud_intelligence_dashboards.quicksight_data_set_refresh_schedule
        LambdaLayerBucketPrefix              = var.cloud_intelligence_dashboards.lambda_layer_bucket_prefix
        DeployCUDOSDashboard                 = var.cloud_intelligence_dashboards.deploy_cudos_dashboard
        DataBucketsKmsKeysArns               = var.cloud_intelligence_dashboards.data_buckets_kms_keys_arns
        DeploymentType                       = var.cloud_intelligence_dashboards.deployment_type
        ShareDashboard                       = var.cloud_intelligence_dashboards.share_dashboard
        KeepLegacyCURTable                   = var.cloud_intelligence_dashboards.keep_legacy_cur_table
        CURBucketPath                        = var.cloud_intelligence_dashboards.cur_bucket_path
        CURTableName                         = var.cloud_intelligence_dashboards.cur_table_name
        PermissionsBoundary                  = var.cloud_intelligence_dashboards.permissions_boundary
        RolePath                             = var.cloud_intelligence_dashboards.role_path
      }
      timeouts = local.default_timeouts
      tags = merge(local.common_tags, {
        DashboardType = "Foundational"
        DashboardId   = "cloud-intelligence-dashboards"
      })
    }
  }

  additional_dashboards = {
    trends = {
      enabled      = var.dashboards.trends == "yes"
      stack_name   = "Trends-Dashboard"
      template_url = local.template_urls.cid_plugin
      capabilities = local.common_capabilities
      parameters   = { DashboardId = "trends-dashboard" }
      timeouts     = local.default_timeouts
      tags = merge(local.common_tags, {
        DashboardType = "Additional"
        DashboardId   = "trends-dashboard"
      })
      dashboard_id = "trends-dashboard"
      dataset_name = "daily-anomaly-detection"
    }
    datatransfer = {
      enabled      = var.dashboards.datatransfer == "yes"
      stack_name   = "DataTransfer-Cost-Analysis-Dashboard"
      template_url = local.template_urls.cid_plugin
      capabilities = local.common_capabilities
      parameters   = { DashboardId = "datatransfer-cost-analysis-dashboard" }
      timeouts     = local.default_timeouts
      tags = merge(local.common_tags, {
        DashboardType = "Additional"
        DashboardId   = "datatransfer-cost-analysis-dashboard"
      })
      dashboard_id = "datatransfer-cost-analysis-dashboard"
      dataset_name = "data_transfer_view"
    }
    marketplace = {
      enabled      = var.dashboards.marketplace == "yes"
      stack_name   = "AWS-Marketplace-SPG-Dashboard"
      template_url = local.template_urls.cid_plugin
      capabilities = local.common_capabilities
      parameters   = { DashboardId = "aws-marketplace", RequiresDataCollection = "no" }
      timeouts     = local.default_timeouts
      tags = merge(local.common_tags, {
        DashboardType = "Additional"
        DashboardId   = "aws-marketplace"
      })
      dashboard_id = "aws-marketplace"
      dataset_name = "marketplace_view"
    }
    connect = {
      enabled      = var.dashboards.connect == "yes"
      stack_name   = "Amazon-Connect-Cost-Insight-Dashboard"
      template_url = local.template_urls.cid_plugin
      capabilities = local.common_capabilities
      parameters   = { DashboardId = "amazon-connect-cost-insight-dashboard" }
      timeouts     = local.default_timeouts
      tags = merge(local.common_tags, {
        DashboardType = "Additional"
        DashboardId   = "amazon-connect-cost-insight-dashboard"
      })
      dashboard_id = "amazon-connect-cost-insight-dashboard"
      dataset_name = "resource_connect_view"
    }
    containers = {
      enabled      = var.dashboards.containers == "yes"
      stack_name   = "SCAD-Containers-Dashboard"
      template_url = local.template_urls.cid_plugin
      capabilities = local.common_capabilities
      parameters   = { DashboardId = "scad-containers-cost-allocation" }
      timeouts     = local.default_timeouts
      tags = merge(local.common_tags, {
        DashboardType = "Additional"
        DashboardId   = "scad-containers-cost-allocation"
      })
      dashboard_id = "scad-containers-cost-allocation"
      dataset_name = "scad_cca_summary_view"
    }
  }

  enabled_additional_dashboards = {
    for k, v in local.additional_dashboards : k => v if v.enabled == "yes"
  }

  # Common tags for all resources
  common_tags = {
    Environment       = var.global_values.environment
    Project           = "cloud-intelligence-dashboards"
    ManagedBy         = "terraform"
    DataExportVersion = var.global_values.data_export_version
    CidCfnVersion     = var.global_values.cid_cfn_version
  }

  # Default timeouts for CloudFormation stacks
  default_timeouts = {
    create = "30m"
    update = "30m"
    delete = "30m"
  }
}
