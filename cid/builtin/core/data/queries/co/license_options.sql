CREATE OR REPLACE VIEW compute_optimizer_license_options AS
(
   SELECT
     TRY("date_parse"(lastrefreshtimestamp_utc, '%Y-%m-%dT%H:%i:%sZ')) lastrefreshtimestamp_utc
   , accountid accountid
   , resourcearn arn
   , TRY("split_part"(resourcearn, ':', 4)) region
   , TRY("split_part"(resourcearn, ':', 3)) service
   , TRY("split_part"(resourcearn, '/', 2)) name
   , 'license' module
   , 'na' recommendationsourcetype
   , finding finding
   , CONCAT(
        (CASE WHEN (findingreasoncodes_isinvalidcloudwatchapplicationinsightssetup = 'true') THEN 'InvalidCloudwatchApplicationInsights ' ELSE '' END),
        (CASE WHEN (findingreasoncodes_iscloudwatchapplicationinsightserror =        'true') THEN 'CloudwatchApplicationInsightsError ' ELSE '' END),
        (CASE WHEN (findingreasoncodes_islicenseoverprovisioned =                    'true') THEN 'LicenseOverprovisioned ' ELSE '' END),
        (CASE WHEN (findingreasoncodes_isoptimized =                                 'true') THEN 'Optimized ' ELSE '' END)
    ) reason
   , lookbackperiodindays lookbackperiodindays
   , 'na' currentperformancerisk
   , errorcode errorcode
   , errormessage errormessage
   , 'na' ressouce_details
   , 'na' utilizationmetrics
   , 'Current' option_name
   , CONCAT(
        currentlicenseconfiguration_numberofcores, ';',
        currentlicenseconfiguration_instancetype, ';',
        currentlicenseconfiguration_licenseversion
        ) option_from
   , '' option_to
   , recommendationoptions_1_estimatedmonthlysavingscurrency currency
   , 0E0 monthlyprice
   , 0E0 hourlyprice
   , COALESCE(TRY_CAST(recommendationoptions_1_estimatedmonthlysavingsvalue AS double), 0E0) as estimatedmonthlysavings_value
   , 0E0 estimatedmonthly_ondemand_cost_change
   , COALESCE(TRY(CAST(recommendationoptions_1_estimatedmonthlysavingsvalue AS double)), 0E0) as max_estimatedmonthlysavings_value_very_low
   , COALESCE(TRY(CAST(recommendationoptions_1_estimatedmonthlysavingsvalue AS double)), 0E0) as max_estimatedmonthlysavings_value_low
   , COALESCE(TRY(CAST(recommendationoptions_1_estimatedmonthlysavingsvalue AS double)), 0E0) as max_estimatedmonthlysavings_value_medium
   , CONCAT(
        CONCAT(COALESCE(currentlicenseconfiguration_licensename, ''), ';'),
        CONCAT(COALESCE(currentlicenseconfiguration_operatingsystem, ''), ';'),
        CONCAT(COALESCE(currentlicenseconfiguration_licenseedition, ''), ';'),
        CONCAT(COALESCE(currentlicenseconfiguration_licensemodel, ''), ';'),
        '', ';'
   ) option_details
   , tags tags
   FROM
     compute_optimizer_license_lines
   WHERE (resourcearn LIKE '%arn:%')
UNION SELECT
     TRY("date_parse"(lastrefreshtimestamp_utc, '%Y-%m-%dT%H:%i:%sZ')) lastrefreshtimestamp_utc
   , accountid accountid
   , resourcearn arn
   , TRY("split_part"(resourcearn, ':', 4)) region
   , TRY("split_part"(resourcearn, ':', 3)) service
   , TRY("split_part"(resourcearn, '/', 2)) name
   , 'license' module
   , '' recommendationsourcetype
   , finding finding
   , '' reason
   , lookbackperiodindays lookbackperiodindays
   , '' currentperformancerisk
   , errorcode errorcode
   , errormessage errormessage
   , '' ressouce_details
   , '' utilizationmetrics
   , 'Recommendation' option_name
   , '' option_from
   , 'na' option_to
   , recommendationoptions_1_estimatedmonthlysavingscurrency currency
   , 0E0 monthlyprice
   , 0E0 hourlyprice
   , COALESCE(TRY(CAST(recommendationoptions_1_estimatedmonthlysavingsvalue AS double)), 0E0) as estimatedmonthlysavings_value
   , COALESCE(TRY_CAST(recommendationoptions_1_estimatedmonthlysavingsvalue AS double), 0E0) as estimatedmonthly_ondemand_cost_change
   , COALESCE(TRY(CAST(recommendationoptions_1_estimatedmonthlysavingsvalue AS double)), 0E0) as max_estimatedmonthlysavings_value_very_low
   , COALESCE(TRY(CAST(recommendationoptions_1_estimatedmonthlysavingsvalue AS double)), 0E0) as max_estimatedmonthlysavings_value_low
   , COALESCE(TRY(CAST(recommendationoptions_1_estimatedmonthlysavingsvalue AS double)), 0E0) as max_estimatedmonthlysavings_value_medium
   , CONCAT(
        CONCAT(COALESCE(currentlicenseconfiguration_licensename, ''), ';'),
        CONCAT(COALESCE(recommendationoptions_1_operatingsystem, ''), ';'),
        CONCAT(COALESCE(recommendationoptions_1_licenseedition, ''), ';'),
        CONCAT(COALESCE(recommendationoptions_1_licensemodel, ''), ';'),
        CONCAT(COALESCE(recommendationoptions_1_savingsopportunitypercentage, ''), ';')
   ) option_details
   , tags tags
   FROM
     compute_optimizer_license_lines
   WHERE (resourcearn LIKE '%arn:%')
 )