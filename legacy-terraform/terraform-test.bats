#!/bin/bats

# Run: bats ./terraform-modules/terraform-test.bats --timing
# Debug: bats ./terraform-modules/terraform-test.bats --timing --show-output-of-passing-tests

# Testing happy path of creating/deleting stack

# prereqs:
#   1. Quicksight Enterprise edition
#   2. Quicksight has rights to read all s3 buckets in the account (not managable today)
#   3. At least 1 CUR created

setup_file() {
  # Vars
  export stackname="tfstack$(date +%Y%m%d%H%M)"
  export cur_prefix="cid$(date +%Y%m%d%H%M)"
  export account_id=$(aws sts get-caller-identity --query Account --output text)
  export quicksight_user=$(aws quicksight list-users --aws-account-id $account_id --namespace default --query 'UserList[0].UserName' --output text)
  export template_bucket=test-cid-tf-template-$account_id

  aws quicksight describe-user --aws-account-id $account_id --user-name $quicksight_user --namespace default
  if [[ "$?" != "0" ]]; then
      echo "Missing QS User '$quicksight_user'"
      return 1
  fi

  export qs_edition=$(aws quicksight describe-account-settings --aws-account-id $account_id --query 'AccountSettings.Edition' --output text)
  if [[ "$qs_edition" != "ENTERPRISE" ]]; then
      echo "Missing ENTERPRISE edition in QuickSight"
      return 1
  fi

  # Create workspace
  export tf_workspace=$(mktemp -d .tftest.XXXXXX)
}


@test "generate terraform manifest" {
  # Create a TF configuration
  git_base_dir=$(git rev-parse --show-toplevel)
  cp -r "$(git rev-parse --show-toplevel)/terraform-modules" "${tf_workspace}"
  cp -r "$(git rev-parse --show-toplevel)/cfn-templates" "${tf_workspace}"
  cat > "$tf_workspace/main.tf" <<EOL
  provider "aws" {}
  provider "aws" {
    region = "us-east-1"
    alias  = "useast1"
  }
  variable "use_kms" {
    type = bool
  }
  resource "aws_kms_key" "s3_key" {
    count                   = var.use_kms ? 1 : 0
    description             = "This key is used to encrypt bucket objects"
    deletion_window_in_days = 7
  }
  resource "aws_s3_bucket" "template_bucket" {
    bucket        = "$template_bucket"
    force_destroy = true
  }
  resource "aws_s3_bucket_server_side_encryption_configuration" "template_bucket" {
    count  = var.use_kms ? 1 : 0
    bucket = aws_s3_bucket.template_bucket.id
    rule {
      apply_server_side_encryption_by_default {
        kms_master_key_id = aws_kms_key.s3_key[0].arn
        sse_algorithm     = "aws:kms"
      }
    }
  }

  module "cur_destination" {
    source             = "./terraform-modules/cur-setup-destination"
    resource_prefix    = "$cur_prefix"
    source_account_ids = ["$account_id"]
    create_cur         = false
    providers = {
      aws.useast1 = aws.useast1
    }
  }
  module "cur_source" {
    source                 = "./terraform-modules/cur-setup-source"
    resource_prefix        = "$cur_prefix"
    destination_bucket_arn = module.cur_destination.cur_bucket_arn

    providers = {
      aws.useast1 = aws.useast1
    }
  }

  module "cid_dashboards" {
    source           = "./terraform-modules/cid-dashboards"
    depends_on       = [aws_s3_bucket_server_side_encryption_configuration.template_bucket]
  
    stack_name      = "$stackname"
    template_bucket = aws_s3_bucket.template_bucket.id
    stack_parameters = {
      "QuickSightDataSourceRoleName"       = ""
      "PrerequisitesQuickSight"            = "yes"
      "PrerequisitesQuickSightPermissions" = "yes"
      "QuickSightUser"                     = "$quicksight_user"
      "DeployCUDOSDashboard"               = "yes"
      "DeployCostIntelligenceDashboard"    = "no"
      "DeployKPIDashboard"                 = "no"
      "CURBucketPath"                      = "s3://\${module.cur_destination.cur_bucket_name}/cur"
      "Suffix"                             = "-test-tf"
    }
  }
EOL
}

@test "terraform init" {
  terraform -chdir="$tf_workspace" init
}

##
# Test Unencrypted Template Bucket
##
@test "terraform apply (takes up to 10 mins)" {
  terraform -chdir="$tf_workspace" apply -auto-approve -var="use_kms=false"
  terraform -chdir="$tf_workspace" state list
}

@test "terraform plan has no changes" {
  terraform -chdir="$tf_workspace" plan -detailed-exitcode -var="use_kms=false"
}

@test "KMS-encrypted template bucket plans with no changes" {
  # Taint the S3 object so it is re-created with KMS encryption
  terraform -chdir="$tf_workspace" taint module.cid_dashboards.aws_s3_object.template
  terraform -chdir="$tf_workspace" apply -auto-approve -var="use_kms=true"
  terraform -chdir="$tf_workspace" plan -detailed-exitcode -var="use_kms=true"
}

@test "cloudformation stack is created successfully" {
  run aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE --query "length(StackSummaries[?StackName==\`$stackname\`])" --output text
  [ "$status" -eq 0 ]
  [ "$output" = "1" ]
}

@test "cur definition is created successfully" {
  run aws cur describe-report-definitions --region us-east-1 --query "length(ReportDefinitions[?ReportName==\`$cur_prefix-cur\`])" --output text
  [ "$status" -eq 0 ]
  [ "$output" = "1" ]
}

@test "source S3 bucket is created successfully" {
  run aws s3api list-buckets --query "length(Buckets[?Name==\`$cur_prefix-$account_id-local\`])" --output text
  [ "$status" -eq 0 ]
  [ "$output" = "1" ]
}

@test "destination S3 bucket is created successfully" {
  run aws s3api list-buckets --query "length(Buckets[?Name==\`$cur_prefix-$account_id-shared\`])" --output text
  [ "$status" -eq 0 ]
  [ "$output" = "1" ]
}

@test "terraform destroy (takes up to 10 mins)" {
  terraform -chdir="$tf_workspace" destroy -auto-approve -var="use_kms=true"
}

@test "cloud formation stack is deleted" {
  run aws cloudformation list-stacks --stack-status-filter DELETE_COMPLETE --query "length(StackSummaries[?StackName==\`$stackname\`])" --output text
  [ "$status" -eq 0 ]
  [ "$output" = "1" ]
}

teardown_file() {
  # Additional teardown steps in case of failure
  export region=$(aws configure get region)
  terraform -chdir="$tf_workspace" destroy -auto-approve -var="use_kms=true" || echo "terraform destroy failed"
  aws s3 rm s3://aws-athena-query-results-cid-$account_id-$region --recursive || echo "no bucket"
  aws s3 rm s3://$cur_prefix-$account_id-local --recursive || echo "no bucket"
  aws s3 rm s3://$cur_prefix-$account_id-shared --recursive || echo "no bucket"
  aws s3api delete-bucket --bucket aws-athena-query-results-cid-$account_id-$region || echo "no bucket"
  aws s3api delete-bucket --bucket $cur_prefix-$account_id-local || echo "no bucket"
  aws s3api delete-bucket --bucket $cur_prefix-$account_id-shared || echo "no bucket"
  aws athena delete-work-group --work-group CID --recursive-delete-option
  aws cur delete-report-definition --region us-east-1 --report-name $cur_prefix-cur

  # Normal cleanup
  aws s3 rm s3://$template_bucket/ --recursive
  aws s3api delete-bucket --bucket $template_bucket
  rm -rf "$tf_workspace"
}