import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the bucket name from environment variables
bucket_name = os.environ.get('BUCKET_NAME')

# Print the bucket name
print(f"Bucket name from environment: {bucket_name}")

# Check if the bucket name is set
if bucket_name:
    print("✅ Successfully loaded bucket name from .env file")
else:
    print("❌ Failed to load bucket name from .env file")