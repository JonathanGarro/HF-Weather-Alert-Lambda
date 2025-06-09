import json
import boto3
import pandas as pd
import os
import tempfile
from datetime import datetime
from dotenv import load_dotenv
from weather_integration import integrate_weather_alerts

# Load environment variables from .env file
load_dotenv()

# AWS clients
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    AWS Lambda function to update weather alerts data
    Triggered by CloudWatch Events every 30 minutes
    """

    print(f"🌦️ Weather alerts update started at {datetime.now()}")

    # Configuration
    bucket_name = os.environ.get('BUCKET_NAME')
    input_key = 'active_org_addresses_geocoded.csv'
    output_key = 'dashboard_with_weather_alerts.csv'

    try:
        # Create temporary files in Lambda's /tmp directory
        input_path = '/tmp/input_data.csv'
        output_path = '/tmp/output_data.csv'

        # Download input data from S3
        print(f"📥 Downloading {input_key} from S3...")
        s3_client.download_file(bucket_name, input_key, input_path)

        # Verify file was downloaded
        if os.path.exists(input_path):
            file_size = os.path.getsize(input_path)
            print(f"✅ Downloaded successfully: {file_size} bytes")
        else:
            raise Exception("Failed to download input file")

        # Run weather alerts integration
        print("🔄 Starting weather alerts integration...")
        success = integrate_weather_alerts(input_path, output_path)

        if success:
            # Verify output file was created
            if os.path.exists(output_path):
                output_size = os.path.getsize(output_path)
                print(f"✅ Integration completed: {output_size} bytes")

                # Upload enhanced data back to S3 with proper Content-Type and permissions
                print(f"📤 Uploading to {output_key}...")
                s3_client.upload_file(
                    output_path, 
                    bucket_name, 
                    output_key,
                    ExtraArgs={
                        'ContentType': 'text/csv',
                        'ACL': 'bucket-owner-full-control',  # Ensure bucket owner can access
                        'Metadata': {
                            'source': 'weather-alerts-lambda',
                            'generated': datetime.now().isoformat()
                        }
                    }
                )
                print("✅ Upload completed successfully with proper Content-Type")

                # Clean up temp files
                os.remove(input_path)
                os.remove(output_path)

                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'Weather alerts integration completed successfully',
                        'timestamp': datetime.now().isoformat(),
                        'input_size': file_size,
                        'output_size': output_size
                    })
                }
            else:
                raise Exception("Integration failed - no output file created")
        else:
            raise Exception("Integration function returned False")

    except Exception as e:
        error_msg = f"Lambda function error: {str(e)}"
        print(f"❌ {error_msg}")

        # Clean up temp files if they exist
        for temp_file in [input_path, output_path]:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except:
                pass

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            })
        }
