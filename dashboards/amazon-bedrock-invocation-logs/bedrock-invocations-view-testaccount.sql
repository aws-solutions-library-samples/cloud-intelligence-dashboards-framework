-- Base bedrock_invocations_view for THIS account (213485169607). No account-name join
-- (the account_name join is done in CUDOS, not in this view).
--
-- Reads raw optimization_data.bedrock_logs and extracts the nested token fields.
-- account_id / payer_id are native columns on bedrock_logs (already CUDOS shape).
-- requestmetadata is map<string,string>; token fields are nested in input/output structs.
--
-- Run in Athena (workgroup CID).

CREATE OR REPLACE VIEW "optimization_data".bedrock_invocations_view AS
SELECT
  account_id
, payer_id
, CAST(from_iso8601_timestamp(timestamp) AS timestamp) timestamp
, region
, requestid
, operation
, modelid
, identity_arn
, IF((identity_arn LIKE '%:assumed-role/%'), element_at(split(identity_arn, '/'), -1), identity_arn) identity
, requestmetadata
, year
, month
, day
, input.inputtokencount input_token_count
, input.inputcontenttype input_content_type
, output.outputtokencount output_token_count
, output.outputcontenttype output_content_type
, CAST(json_extract_scalar(output.outputbodyjson, '$.usage.inputtokens') AS INTEGER) usage_input_tokens
, CAST(json_extract_scalar(output.outputbodyjson, '$.usage.outputtokens') AS INTEGER) usage_output_tokens
, CAST(json_extract_scalar(output.outputbodyjson, '$.usage.totaltokens') AS INTEGER) usage_total_tokens
, CAST(json_extract_scalar(output.outputbodyjson, '$.usage.cachereadinputtokencount') AS INTEGER) usage_cache_read_tokens
, CAST(json_extract_scalar(output.outputbodyjson, '$.usage.cachewriteinputtokencount') AS INTEGER) usage_cache_write_tokens
, json_extract_scalar(output.outputbodyjson, '$.stopreason') stop_reason
, json_extract_scalar(output.outputbodyjson, '$.output.message.content[0].text') response_preview
, COALESCE(json_extract_scalar(input.inputbodyjson, '$.messages[0].content[0].text'), json_extract_scalar(input.inputbodyjson, '$.messages[0].content'), json_extract_scalar(input.inputbodyjson, '$.prompt'), json_extract_scalar(input.inputbodyjson, '$.inputText')) prompt_preview
FROM
  "optimization_data".bedrock_logs
WHERE (from_iso8601_timestamp(timestamp) >= date_add('month', -6, current_timestamp))
