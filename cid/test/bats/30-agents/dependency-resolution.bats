#!/bin/bats

# Dependency-resolution seeding tests for create-agent
# (Requirements 8.3, 8.5, 8.6, 19.1, 19.3).
#
# create-agent is prerequisite-only: it NEVER deploys dashboards. These tests
# assert the command's output contract against the account's seeded state:
#
#   * Zero present:  no CID dashboards deployed  -> non-zero exit + guidance
#                    error naming the CID/CUDOS workshop and Data Collection
#                    guidance URLs, naming no cid-cmd command, deploying nothing
#   * Partial:       a strict subset of the finops agent's dependency dashboards
#                    deployed -> exit 0 with per-missing-dependency warnings,
#                    deploying nothing
#
# Seeding prerequisites (full seeding automation is impractical here, so each
# test self-gates on the live account state and skips when it does not match):
#
#   Zero-present case:  a sandbox account with QuickSight Enterprise active and
#                       ZERO QuickSight dashboards deployed.
#   Partial case:       seed a subset first, e.g. deploy exactly one required
#                       dependency of the finops agent:
#                         cid-cmd deploy --dashboard-id cudos-v5 ...
#                       (see 10-deploy-update-delete/cudos.bats for full flags)
#                       and leave at least one of cost_intelligence_dashboard /
#                       kpi_dashboard / trends-dashboard NOT deployed.

account_id=$(aws sts get-caller-identity --query "Account" --output text 2>/dev/null || true)
agent_catalog_key="${agent_catalog_key:-finops}"    # catalog key used with --agent-id
agent_id="${agent_id:-cid-finops-advisor}"          # deployed agentId of that catalog entry
# agents/spaces are catalog content; load the repo's local catalog so the tests
# exercise the checked-out definitions rather than the published main branch
catalog="${catalog:-$BATS_TEST_DIRNAME/../../../../agents/catalog.yaml}"

# dependency dashboards of the finops agent (dashboardIds as deployed):
# required: cudos-v5, cost_intelligence_dashboard, kpi_dashboard; optional: trends-dashboard
required_dashboard_ids="cudos-v5 cost_intelligence_dashboard kpi_dashboard"
optional_dashboard_ids="trends-dashboard"

function skip_without_aws {
  if ! aws sts get-caller-identity >/dev/null 2>&1; then
    skip "AWS credentials are not available"
  fi
}

function dashboards_count {
  aws quicksight list-dashboards \
    --aws-account-id "$account_id" \
    --query 'length(DashboardSummaryList)' \
    --output text
}

function dashboard_present {
  aws quicksight describe-dashboard \
    --aws-account-id "$account_id" \
    --dashboard-id "$1" >/dev/null 2>&1
}

@test "Zero present: create-agent errors with guidance and deploys nothing" {
  skip_without_aws
  before=$(dashboards_count)
  if [ "$before" != "0" ]; then
    skip "Account has $before dashboards deployed; this test needs a sandbox with zero CID dashboards"
  fi

  run cid-cmd -vv --yes create-agent \
    --agent-id $agent_catalog_key \
    --catalog "$catalog"

  # guidance error, non-zero exit
  [ "$status" -ne 0 ]
  echo "$output" | grep -q 'No CID dashboards'
  # points to the CID/CUDOS foundational and Data Collection guidance
  echo "$output" | grep -q 'https://catalog.workshops.aws/awscid/en-US'
  echo "$output" | grep -q 'https://catalog.workshops.aws/awscid/en-US/data-collection'
  # the guidance error itself names no cid-cmd command
  echo "$output" | grep 'No CID dashboards' | grep -v -q 'cid-cmd'

  # nothing was deployed: dashboard count unchanged (still zero)
  after=$(dashboards_count)
  [ "$after" = "0" ]

  # and no agent was created
  run cid-cmd list-agents --catalog "$catalog"
  [ "$status" -eq 0 ]
  if echo "$output" | grep -qF "✓[$agent_id]"; then
    echo "agent $agent_id was created despite the zero-present guidance error"
    return 1
  fi
}

@test "Partial: create-agent proceeds with warnings and deploys nothing" {
  skip_without_aws
  present=0
  missing=0
  for id in $required_dashboard_ids; do
    if dashboard_present "$id"; then present=$((present+1)); else missing=$((missing+1)); fi
  done
  for id in $optional_dashboard_ids; do
    if dashboard_present "$id"; then :; else missing=$((missing+1)); fi
  done
  if [ "$present" -eq 0 ]; then
    skip "No finops dependency dashboard is deployed; seed a subset first (see header comments)"
  fi
  if [ "$missing" -eq 0 ]; then
    skip "All finops dependency dashboards are deployed; this test needs a strict subset"
  fi
  before=$(dashboards_count)

  run cid-cmd -vv --yes create-agent \
    --agent-id $agent_catalog_key \
    --catalog "$catalog"

  # proceeds with the present subset
  [ "$status" -eq 0 ]
  # a warning is emitted for each missing dependency, pointing to the guidance
  echo "$output" | grep -q 'Warning:'
  echo "$output" | grep -q 'is not deployed'
  echo "$output" | grep -q 'Proceeding with the available dashboards'
  echo "$output" | grep -q 'https://catalog.workshops.aws/awscid'
  # the warnings name no cid-cmd command
  echo "$output" | grep 'is not deployed' | grep -v -q 'cid-cmd'

  # nothing was deployed: dashboard count unchanged
  after=$(dashboards_count)
  [ "$after" = "$before" ]

  # cleanup: remove the agent created by this test
  run cid-cmd -vv delete-agent \
    --agent-id $agent_catalog_key \
    --catalog "$catalog" \
    --confirm-delete yes
  [ "$status" -eq 0 ]
}
