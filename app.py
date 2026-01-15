import sys
import boto3
import time
import json
from awsglue.utils import getResolvedOptions

# Get arguments
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_BUCKET_NAME',
    'REDSHIFT_ENDPOINT',
    'REDSHIFT_ROLE_ARN',
    'REDSHIFT_SECRET_ARN',
    'SOURCE_FILE_PATH', # New: e.g. "folder/data.json"
    'STAGING_TABLE',    # New: e.g. "schema.staging_table"
    'FINAL_TABLE'       # New: e.g. "schema.final_table"
])

s3_bucket = args['S3_BUCKET_NAME']
endpoint = args['REDSHIFT_ENDPOINT']
role_arn = args['REDSHIFT_ROLE_ARN']
secret_arn = args['REDSHIFT_SECRET_ARN']
db_name = 'dev'

# Table Definitions from Args
staging_table = args['STAGING_TABLE']
final_table = args['FINAL_TABLE']
source_file_key = args['SOURCE_FILE_PATH']

# Extract workgroup and region from endpoint
parts = endpoint.split('.')
workgroup_name = parts[0]
region = parts[2]

# --- JSON Array Preprocessing ---
s3_client = boto3.client('s3')

print(f"Checking file format for: s3://{s3_bucket}/{source_file_key}")

try:
    # Read file content
    response = s3_client.get_object(Bucket=s3_bucket, Key=source_file_key)
    file_content = response['Body'].read().decode('utf-8')
    
    # Try to parse as JSON
    try:
        data = json.loads(file_content)
        
        # If it is a list (JSON Array), convert to NDJSON
        if isinstance(data, list):
            print("Detected JSON Array. Converting to NDJSON...")
            ndjson_content = '\n'.join([json.dumps(record) for record in data])
            
            # Upload processed file
            processed_key = f"processed/{source_file_key}"
            s3_client.put_object(Bucket=s3_bucket, Key=processed_key, Body=ndjson_content)
            
            # Update s3_path to point to processed file
            s3_path = f"s3://{s3_bucket}/{processed_key}"
            print(f"Uploaded converted file to: {s3_path}")
            
        else:
            # It's a single object or not a list, use original
            print("File is valid JSON but not an array (likely single object). Using original.")
            s3_path = f"s3://{s3_bucket}/{source_file_key}"
            
    except json.JSONDecodeError:
        # If json.loads fails, it might be NDJSON already (multiple root objects)
        print("File is not valid single JSON object (likely NDJSON). Using original.")
        s3_path = f"s3://{s3_bucket}/{source_file_key}"

except Exception as e:
    print(f"Error reading/processing file: {e}")
    raise e

# --- Redshift Execution ---
print(f"Connecting to Workgroup: {workgroup_name} in Region: {region}")
client = boto3.client('redshift-data', region_name=region)

# Define SQL statements for Merge/Upsert Pattern
# 1. Truncate Staging (Clean slate)
truncate_staging_sql = f"TRUNCATE TABLE {staging_table};"

# 2. Copy to Staging (Use processed s3_path)
copy_staging_sql = f"""
    COPY {staging_table}
    FROM '{s3_path}'
    IAM_ROLE '{role_arn}'
    FORMAT AS JSON 'auto';
"""

# 3. Create Final Table (if not exists)
create_final_sql = f"CREATE TABLE IF NOT EXISTS {final_table} (LIKE {staging_table});"

# 4. MERGE (Upsert) - Native Redshift Command
merge_sql = f"""
    MERGE INTO {final_table} USING {staging_table}
    ON {final_table}.id = {staging_table}.id
    WHEN MATCHED THEN UPDATE SET
        name = {staging_table}.name,
        email = {staging_table}.email,
        age = {staging_table}.age,
        created_at = {staging_table}.created_at
    WHEN NOT MATCHED THEN INSERT (id, name, email, age, created_at)
    VALUES ({staging_table}.id, {staging_table}.name, {staging_table}.email, {staging_table}.age, {staging_table}.created_at);
"""

statements = [
    ("Truncate Staging", truncate_staging_sql),
    ("Copy to Staging", copy_staging_sql),
    ("Create Final Table", create_final_sql),
    ("Merge (Upsert) Data", merge_sql)
]

for name, sql in statements:
    print(f"Executing: {name}")
    print(sql)
    
    try:
        response = client.execute_statement(
            WorkgroupName=workgroup_name,
            Database=db_name,
            SecretArn=secret_arn,
            Sql=sql
        )
        
        statement_id = response['Id']
        print(f"Statement ID: {statement_id}")
        
        # Poll for completion
        while True:
            status_resp = client.describe_statement(Id=statement_id)
            status = status_resp['Status']
            
            if status in ['FAILED', 'ABORTED']:
                error_msg = status_resp.get('Error', 'Unknown Error')
                print(f"Error in {name}: {error_msg}")
                raise Exception(f"{name} Failed: {error_msg}")
                
            elif status == 'FINISHED':
                print(f"{name} Finished Successfully.")
                break
                
            time.sleep(2)
            
    except Exception as e:
        print(f"Execution failed at step {name}: {e}")
        raise e

print("Job Completed Successfully.")
