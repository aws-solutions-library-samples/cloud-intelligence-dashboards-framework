CREATE OR REPLACE VIEW compute_optimizer_idle_options AS
(
   SELECT
     TRY("date_parse"(lastrefreshtimestamp_utc, '%Y-%m-%dT%H:%i:%sZ')) lastrefreshtimestamp_utc
   , accountid accountid
   , resourcearn arn
   , TRY("split_part"(resourcearn, ':', 4)) region
   , TRY("split_part"(resourcearn, ':', 3)) service
   , resourceid name
   , 'idle' module
   , resourcetype recommendationsourcetype
   , finding finding
   , findingdescription reason
   , lookbackperiodindays lookbackperiodindays
   , 'na' currentperformancerisk
   , errorcode errorcode
   , errormessage errormessage
   , '' ressouce_details
   , CONCAT(
        utilizationmetricscpumaximum, ';',
        utilizationmetricsmemorymaximum, ';',
        utilizationmetricsnetworkoutbytespersecondmaximum, ';',
        utilizationmetricsnetworkinbytespersecondmaximum, ';',
        utilizationmetricsdatabaseconnectionsmaximum, ';',
        utilizationmetricsebsvolumereadiopsmaximum, ';',
        utilizationmetricsebsvolumewriteiopsmaximum, ';',
        utilizationmetricsvolumereadopspersecondmaximum, ';',
        utilizationmetricsvolumewriteopspersecondmaximum
        ) utilizationmetrics
   , 'Current' option_name
   , 'na' option_from
   , '' option_to
   , estimatedmonthlysavingscurrency currency
   , 0E0 monthlyprice
   , 0E0 hourlyprice
   , TRY(CAST(estimatedmonthlysavingsvalue AS double)) estimatedmonthlysavings_value
   , 0E0 estimatedmonthly_ondemand_cost_change
   , TRY(CAST(estimatedMonthlySavingsValue AS double)) max_estimatedmonthlysavings_value_very_low
   , TRY(CAST(estimatedMonthlySavingsValue AS double)) max_estimatedmonthlysavings_value_low
   , TRY(CAST(estimatedMonthlySavingsValue AS double)) max_estimatedmonthlysavings_value_medium
   , CONCAT(
        COALESCE(recommendations_count, ''), ';',
        COALESCE(savingsopportunitypercentage, ''), ';'
   ) option_details
   , tags tags
   FROM
     compute_optimizer_idle_lines
   WHERE (resourcearn LIKE '%arn:%')
UNION SELECT
     TRY("date_parse"(lastrefreshtimestamp_utc, '%Y-%m-%dT%H:%i:%sZ')) lastrefreshtimestamp_utc
   , accountid accountid
   , resourcearn arn
   , TRY("split_part"(resourcearn, ':', 4)) region
   , TRY("split_part"(resourcearn, ':', 3)) service
   , resourceid name
   , 'idle' module
   , resourcetype recommendationsourcetype
   , finding finding
   , findingdescription reason
   , lookbackperiodindays lookbackperiodindays
   , '' currentperformancerisk
   , errorcode errorcode
   , errormessage errormessage
   , 'na' ressouce_details
   , '' utilizationmetrics
   , 'Recommendation' option_name
   , '' option_from
   , 'na' option_to
   , estimatedmonthlysavingscurrency currency
   , 0E0 monthlyprice
   , 0E0 hourlyprice
   , TRY(CAST(estimatedmonthlysavingsvalue AS double)) estimatedmonthlysavings_value
   , 0E0 estimatedmonthly_ondemand_cost_change
   , TRY(CAST(estimatedMonthlySavingsValue AS double)) max_estimatedmonthlysavings_value_very_low
   , TRY(CAST(estimatedMonthlySavingsValue AS double)) max_estimatedmonthlysavings_value_low
   , TRY(CAST(estimatedMonthlySavingsValue AS double)) max_estimatedmonthlysavings_value_medium
   , CONCAT(
        COALESCE(estimatedmonthlysavingsvalueafterdiscounts, ''), ';',
        COALESCE(savingsopportunitypercentageafterdiscounts, ''), ';' 
   ) option_details
   , tags tags
   FROM
     compute_optimizer_idle_lines
   WHERE (resourcearn LIKE '%arn:%')
 )