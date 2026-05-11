# What's new in Health Events Dashboard (HED)

## v3.0.0 (April 2026) -- NOTE: This requires updating the Data Collection Framework beyond version 3.14.4

- New Health fields for Actionability and Personas added
- Significant revision of user interface for landing page
- Emphasis for reporting on AWS Planned Lifecycle Events (PLEs), such as servive version deprecations

## v2.1.0

- Modified Athena query to include events ingested more than 90 days ago if they are not of closed status. Although not a breaking change for the dasboard, you should update with the `--force --recursive` flags to incorporate it.
- Added guidance text for date range filtering
- Minor cosmetic and usability changes

## v2.0.4

- Fix resetting description free text filter issue
- Minor cosmetic and usability changes

## v2.0.3

- Fix case where reported resource name is not a full unique ARN to derive unique instance counts
- Easier filtering by date ranges
- Layout changes to event detail section
- Minor cosmetic and usage guidance changes

## v2.0.2

- Fix Event Category filter on Events Explorer tab
- Minor cosmetic changes
- Add Event Start Date parameter to assist filtering out old events

## v2.0.1

- Minor cosmetic changes and visual fix

## v2.0.0

- Reorganized Summary tab for better flow and easy creation of targeted inventory reports of impacted resources
- _NOTE: This and any updates from v1.x to v2.x requires that you update with cid-cmd parameters: --force --recursive_

## v1.0.5

- Added icons and styling for status

## v1.0.4

- Switched Status to Resource Status from Event Status
- Added Affected resources list

## v1.0.3

- Enhanced Events page

## v1.0.2

- Initial release
