# Weather Alerts Lambda Function

## Description
This AWS Lambda function integrates weather alerts from the National Weather Service (NWS) API with organization address data for the Grantmaking Hazards Dashboard in Tableau, which helps us monitor which grantees are potentially affected by major weather disruptions.


## Features
- Automatically fetches active weather alerts from the NWS API
- Matches alerts to organizations using multiple strategies:
  - Alert ID matching
  - Zone URL matching
  - State mapping
  - Text content matching
- Enhances organization data with weather alert information:
  - Alert type, severity, and urgency
  - Alert headlines and descriptions
  - Affected areas
  - Effective and expiration times
- Calculates severity scores and alert counts
- Outputs a CSV file ready for Tableau dashboards

## Architecture
The function follows this workflow:
1. Downloads organization address data from an S3 bucket
2. Fetches active weather alerts from the NWS API
3. Matches alerts to organizations based on CWA regions
4. Enhances the dataset with weather alert information
5. Uploads the enhanced dataset back to S3

## Installation

### Prerequisites
- AWS account with Lambda and S3 access
- Python 3.8 or higher
- Required Python packages: pandas, requests, boto3

### Setup
1. Clone this repository
2. Install dependencies:
   ```
   pip install pandas requests boto3
   ```
3. Create a ZIP file containing all the code files and dependencies
4. Create an AWS Lambda function with Python 3.8+ runtime
5. Upload the ZIP file to the Lambda function
6. Configure environment variables (if needed)
7. Set up a CloudWatch Events trigger to run every 30 minutes

## Configuration
The function uses environment variables for configuration. Create a `.env` file in the project root with the following variables:

```
BUCKET_NAME=your-s3-bucket-name
```

The following configuration is still in `lambda_function.py`:
```python
input_key = 'active_org_addresses_geocoded.csv'
output_key = 'dashboard_with_weather_alerts.csv'
```

When deploying to AWS Lambda, set the `BUCKET_NAME` environment variable in the Lambda function configuration.

### Input Data Requirements
The input CSV file must contain:
- Organization addresses with geocoded information
- A column identifying the CWA region (possible column names: "CWA_Region", "CWA_region", "CWA", "Weather_Office", "NWS_Office")

## Usage
The Lambda function runs automatically based on the CloudWatch Events trigger. You can also invoke it manually through the AWS Lambda console or CLI.

### Testing Locally
To test the function locally:
1. Set up AWS credentials in your environment
2. Create a test script that calls the lambda_handler function
3. Run the script with your test event

## Output
The function produces a CSV file with the original organization data enhanced with weather alert information, including:
- Alert IDs and types
- Severity, urgency, and certainty levels
- Alert headlines and descriptions
- Effective and expiration times
- Severity scores and alert counts

## Security Notes
- The function uses the AWS Lambda execution role for S3 access
- No API keys are required for the NWS API
- Ensure proper S3 bucket permissions
