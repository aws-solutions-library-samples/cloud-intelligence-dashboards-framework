#!/bin/bats

# Quick Agent create -> list -> delete round-trip (Requirement 1.1).
#
# Prerequisites for the round-trip tests (everything except the --help test):
#   * AWS credentials for a sandbox account (tests skip cleanly when absent)
#   * QuickSight Enterprise subscription active
#   * A region/partition where Quick Suite gen-AI APIs are available
#   * The caller registered as a QuickSight user with an agent-capable role (Author Pro)
#   * At least one of the finops agent dependency dashboards already deployed
#     (cudos-v5 / cost_intelligence_dashboard / kpi_dashboard) — create-agent is
#     prerequisite-only and never deploys dashboards
#
# The "--help lists agent commands" test runs WITHOUT AWS credentials.

account_id=$(aws sts get-caller-identity --query "Account" --output text 2>/dev/null || true)
agent_catalog_key="${agent_catalog_key:-finops}"           # catalog key used with --agent-id
agent_id="${agent_id:-cid-finops-advisor}"                 # deployed agentId of that catalog entry
# agents/spaces are catalog content; load the repo's local catalog so the tests
# exercise the checked-out definitions rather than the published main branch
catalog="${catalog:-$BATS_TEST_DIRNAME/../../../../agents/catalog.yaml}"

# Skip credential-dependent tests when no AWS credentials are available,
# so the credential-independent assertions always run.
function skip_without_aws {
  if ! aws sts get-caller-identity >/dev/null 2>&1; then
    skip "AWS credentials are not available"
  fi
}

@test "--help lists create-agent, list-agents and delete-agent" {
  # Runs without AWS credentials
  run cid-cmd --help

  [ "$status" -eq 0 ]
  echo "$output" | grep -q 'create-agent'
  echo "$output" | grep -q 'list-agents'
  echo "$output" | grep -q 'delete-agent'
}

@test "create-agent deploys the agent (round-trip step 1)" {
  skip_without_aws

  run cid-cmd -vv --yes create-agent \
    --agent-id $agent_catalog_key \
    --catalog "$catalog"

  [ "$status" -eq 0 ]
}

@test "list-agents marks the agent as deployed (round-trip step 2)" {
  skip_without_aws

  run cid-cmd -vv list-agents --catalog "$catalog"

  [ "$status" -eq 0 ]
  # deployed entries carry the check indicator: ' ✓[<agentId>] <name>'
  echo "$output" | grep -F "✓[$agent_id]"
}

@test "delete-agent removes the agent (round-trip step 3)" {
  skip_without_aws

  run cid-cmd -vv delete-agent \
    --agent-id $agent_catalog_key \
    --catalog "$catalog" \
    --confirm-delete yes

  [ "$status" -eq 0 ]
}

@test "list-agents no longer marks the agent as deployed" {
  skip_without_aws

  run cid-cmd -vv list-agents --catalog "$catalog"

  [ "$status" -eq 0 ]
  # the entry is still listed from the catalog, but without the check indicator
  if echo "$output" | grep -qF "✓[$agent_id]"; then
    echo "agent $agent_id is still marked as deployed after delete-agent"
    return 1
  fi
}
