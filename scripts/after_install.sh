#!/bin/bash
echo "AfterInstall hook: Installing dependencies for ETL script."
cd /home/ec2_user/db_ingest # Navigate to the deployed script directory

# Example for Python: Create a virtual environment and install dependencies
# This assumes your requirements.txt is in the root of your deployment package
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
deactivate

# If your script requires specific permissions or environment variables set up on the EC2 instance, do it here.
