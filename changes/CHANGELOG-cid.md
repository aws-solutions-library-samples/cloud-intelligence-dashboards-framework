# What's new in the Cost Intelligence Dashboard (CID) 

## CID - 3.6
* Added support for organizational taxonomy

## CID - 3.5
* Removed link to deprecated feedback form

## CID - 3.4
* All sheets
	* Added Payer Accounts control

## CID - 3.3
* RI/SP Summary
	* Fixed Compute SP Total Savings for current Month in "Pricing Model Savings Summary" Visual

## CID - 3.2
* Billing summary 
	* Limited Forecast
* Compute Summary
	* Fixed spot savings visual.
    
## CID - 3.1
* All
	* Added Account Name control to all tabs
* Compute Summary
	* Removed % sign from Spot savings.

## CID - 3
* All 
	* Added an Account ID control to CID 
	* New calc fields or calc field adjustments
	* Normalized Usage Quantity 
	* % spot savings
	* Spot coverage
	* Spot Savings 
	* Spot OnDemand Cost
	* % RI_SP savings
	* Date Granularity
	* OnDemand  Cost
	* OnDemand Coverage
	* Open Invoice Count
	* RI_SP Coverage Tracking
	* RI_SP OnDemand Cost
	* RI_SP Coverage
	* RI_SP Utilization
	* Renamed calc fields % coverage to % coverage usage
	* Added a % coverage cost
	* Added a Product Category to summary view to group services and provide a better grouping for SPs/RIs.
	* Containers called as compute
	* Renamed Group by Fields to Group By Fields - Level 1
	* Added a Group By Fields - Level 2 for OPTICS explorer
	* Updated the EBS volume field to include gp3 and io2 better naming
* Billing summary
	* Added the invoice id insight from CUDOS
	* Added an AWS monthly report visual
* Compute & RI/SP
	* Changed RI/SP to total savings not just gross RI/SP savings.
	* Updated Spot visual to summary view not ec2 running costs now that we have spot fields 	available. To avoid negative savings for fargate spot added formula to take the amortized cost for those sum(ifelse(({purchase_option}='Spot' AND {Cost_Public} >0),{Cost_Public},{purchase_option}='Spot',{Cost_Amortized},0))
	* Updated the RI/SP summary to include a filter for product category and service
	* Added in new RI/SP visuals at the bottom with a date filter for those. 
	* Updated the Expiring RI/SP tracker to have more data points in the table and include a grouping for Product Category 
* Optics Explorer
	* Added a group by level 2 field to allow for multi-dimensional reporting
	* Added more filters to align with CE better and more!
	* Changed date filtering to relative filter so we could do trailing, etc.
	* Added a date granularity to provide daily or monthly views


