-- This is an example of Amazon Athena veiw for consolidation of AWS, Azure, GCP and OCI data from respective Athena tables. 
-- Leave unions only for FOCUS tables that you have deployed. See more https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/focus-dashboard.html#add-focus-data-from-other-cloud-providers-to-focus-dashboard.
-- Update database and table names if you are using names different from the defaults.
-- IMPORTANT: Null values are used in unions from some cloud providers when a column is missing in their FOCUS data. If the respective provider begins delivering the column, you need to update this view to use the actual column value instead of null. Also please raise an issue to this repository to notify us as well.


CREATE OR REPLACE VIEW "cid_cur"."focus_consolidation_view" AS 
SELECT
  AvailabilityZone
, BilledCost
, BillingAccountId
, BillingAccountName
, BillingCurrency
, BillingPeriodEnd
, BillingPeriodStart
, ChargeCategory
, ChargeClass
, ChargeDescription
, ChargeFrequency
, ChargePeriodEnd
, ChargePeriodStart
, CommitmentDiscountCategory
, CommitmentDiscountId
, CommitmentDiscountName
, CommitmentDiscountType
, CommitmentDiscountStatus
, ConsumedQuantity
, ConsumedUnit
, ContractedCost
, ContractedUnitPrice
, EffectiveCost
, InvoiceIssuerName
, ListCost
, ListUnitPrice
, PricingCategory
, PricingQuantity
, PricingUnit
, ProviderName
, PublisherName
, RegionId
, RegionName
, ResourceId
, ResourceName
, ResourceType
, ServiceCategory
, ServiceName
, SkuId
, SkuPriceId
, SubAccountId
, SubAccountName
, Tags
, billing_period
FROM
  "cid_data_export"."focus"

-- Include Azure FOCUS table. Remove this union if you don't have Azure FOCUS table deployed
UNION ALL 

SELECT
  null AvailabilityZone
, BilledCost
, BillingAccountId
, BillingAccountName
, BillingCurrency
, BillingPeriodEnd
, BillingPeriodStart
, ChargeCategory
, ChargeClass
, ChargeDescription
, ChargeFrequency
, ChargePeriodEnd
, ChargePeriodStart
, CommitmentDiscountCategory
, CommitmentDiscountId
, CommitmentDiscountName
, CommitmentDiscountType
, CommitmentDiscountStatus
, ConsumedQuantity
, ConsumedUnit
, ContractedCost
, ContractedUnitPrice
, EffectiveCost
, InvoiceIssuerName
, ListCost
, ListUnitPrice
, PricingCategory
, PricingQuantity
, PricingUnit
, ProviderName
, PublisherName
, RegionId
, RegionName
, ResourceId
, ResourceName
, ResourceType
, ServiceCategory
, ServiceName
, SkuId
, SkuPriceId
, SubAccountId
, SubAccountName
, Tags
, billing_period
FROM
  "cidgldpdcidazure"."cidgltpdcidazure"

-- Include OCI FOCUS table. Remove this union if you don't have OCI FOCUS table deployed
UNION ALL

SELECT
  AvailabilityZone
, BilledCost
, BillingAccountId
, BillingAccountName
, BillingCurrency
, BillingPeriodEnd
, BillingPeriodStart
, ChargeCategory
, null ChargeClass
, ChargeDescription
, ChargeFrequency
, ChargePeriodEnd
, ChargePeriodStart
, CommitmentDiscountCategory
, CommitmentDiscountId
, CommitmentDiscountName
, CommitmentDiscountType
, null CommitmentDiscountStatus
, null ConsumedQuantity
, null ConsumedUnit
, null ContractedCost
, null ContractedUnitPrice
, EffectiveCost
, invoiceissuer InvoiceIssuerName
, ListCost
, ListUnitPrice
, PricingCategory
, PricingQuantity
, PricingUnit
, provider ProviderName
, publisher PublisherName
, region RegionId
, region RegionName
, ResourceId
, ResourceName
, ResourceType
, ServiceCategory
, ServiceName
, SkuId
, SkuPriceId
, SubAccountId
, SubAccountName
, Tags
, billing_period
FROM
  "cid_oci_focus_data_export_dv"."focus"

-- Include GCP FOCUS table. Remove this union if you don't have GCP FOCUS table deployed
UNION ALL

SELECT
  AvailabilityZone
, BilledCost
, BillingAccountId
, null BillingAccountName
, BillingCurrency
, null BillingPeriodEnd
, CAST(BillingPeriodStart AS Date) BillingPeriodStart
, ChargeCategory
, ChargeClass
, ChargeDescription
, null ChargeFrequency
, null ChargePeriodEnd
, CAST(ChargePeriodStart AS date) ChargePeriodStart
, CommitmentDiscountCategory
, CommitmentDiscountId
, CommitmentDiscountName
, null CommitmentDiscountType
, null CommitmentDiscountStatus
, ConsumedQuantity
, ConsumedUnit
, ContractedCost
, ContractedUnitPrice
, EffectiveCost
, null InvoiceIssuerName
, ListCost
, ListUnitPrice
, PricingCategory
, PricingQuantity
, PricingUnit
, ProviderName
, PublisherName
, RegionId
, RegionName
, null ResourceId
, null ResourceName
, null ResourceType
, element_at(ServiceCategory, 2) ServiceCategory
, ServiceName
, SkuId
, SkuPriceId
, SubAccountId
, null SubAccountName
, null Tags
, date_format(CAST(BillingPeriodStart AS date), '%Y-%m') billing_period
FROM
  "gcpapp_db"."gcp_focus_export"
