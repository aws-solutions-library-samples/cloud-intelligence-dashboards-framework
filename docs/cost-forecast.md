This PR adds AWS Cost Explorer forecast functionality to the CUDOS framework. This implementation:

   1. Creates a Lambda function that generates cost forecasts using the Cost Explorer API
   2. Stores forecast data in S3 in a format compatible with QuickSight
   3. Generates a QuickSight manifest file for easy data import
   4. Updates daily on a schedule
   5. Provides outputs with S3 paths for the data and manifest

   **New Parameters:**
   - `DeployForecastDashboard`: Enable/disable forecast functionality (default: no)
   - `ForecastPeriod`: Number of days to forecast (default: 90)
   - `ForecastGranularity`: DAILY or MONTHLY granularity (default: DAILY)
   - `ForecastMetrics`: Metrics to forecast (default: UNBLENDED_COST,BLENDED_COST)
   - `ForecastDimensions`: Dimensions to forecast by (default: SERVICE,LINKED_ACCOUNT,REGION)
   - `ForecastScheduleExpression`: Schedule for forecast data refresh (default: daily at 1 AM UTC)

   **Manual QuickSight Integration:**
   After deploying the template with forecast functionality enabled:
   1. Navigate to QuickSight → Datasets → New dataset
   2. Select S3 as the data source
   3. Use the manifest URL from the CloudFormation outputs (ForecastManifestPath)
   4. Create visualizations based on the forecast data

   **Documentation:**
   - Added documentation in docs/cost-forecast.md

   **Testing:**
   - Tested in both development and production environments
   - Verified forecast data generation
   - Verified QuickSight data import
