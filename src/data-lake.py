import boto3
import json
import time #provides delays
import requests
from dotenv import load_dotenv
import os

load_dotenv()

#CONFIGURATIONS
region = 'eu-west-1'
bucket_name = os.getenv('S3_BUCKET_NAME')
glue_database = 'glue_nba_data_lake'
athena_output_location = f"s3://{bucket_name}/athena-results/"
api_key = os.getenv('SPORTS_API_KEY')

nba_endpoint = os.getenv('NBA_ENDPOINT')

s3_client = boto3.client('s3', region_name=region)
athena_client = boto3.client('athena', region_name=region)
glue_client = boto3.client('glue', region_name = region)


def create_s3_bucket():
    """ create s3 bucket to store sports data"""
    try:
        if region == 'us-east-1':
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region})
    except Exception as e:
        print(f"Error creating S3 bucket: {e}")

def create_glue_db():
    """ create glue db for data lake
    """
    try:
        glue_client.create_database(DatabaseInput={
            "Name": glue_database,
            "Description": "Glue database for NBA sports analytics"
        })
        print(f"Glue database: {glue_database} successfully created.")
    except Exception as e:
        print(f"Error creating glue database: {e}")

def fetch_nba_data():
    "fetch nba data from sportsdata.io"
    try:
        headers= {"Ocp-Apim-Subscription-Key": api_key}
        response = requests.get(nba_endpoint, headers=headers)
        response.raise_for_status() 
        print('Successfully fetched NBA data.')
        return response.json() #return json response

    except Exception as e:
        print("Error fetching data: ", e)
        return []
    
def convert_to_line_delimited_json(data):
    """Convert data to line-delimited JSON format."""

    print("Converting data to line-delimited JSON format...")
    return "\n".join([json.dumps(record) for record in data])


def upload_data_to_s3(data):
    """Upload NBA data to the S3 bucket."""
    try:
        # Convert data to line-delimited JSON
        line_delimited_data = convert_to_line_delimited_json(data)

        # Define S3 object key
        file_key = "raw-data/nba_player_data.json"

        # Upload JSON data to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=line_delimited_data
        )
        print(f"Uploaded data to S3: {file_key}")
    except Exception as e:
        print(f"Error uploading data to S3: {e}")

def create_glue_table():
    """Create a Glue table for the data."""
    try:
        glue_client.create_table(
            DatabaseName=glue_database,
            TableInput={
                "Name": "nba_players",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "PlayerID", "Type": "int"},
                        {"Name": "FirstName", "Type": "string"},
                        {"Name": "LastName", "Type": "string"},
                        {"Name": "Team", "Type": "string"},
                        {"Name": "Position", "Type": "string"},
                        {"Name": "Points", "Type": "int"}
                    ],
                    "Location": f"s3://{bucket_name}/raw-data/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"
                    },
                },
                "TableType": "EXTERNAL_TABLE",
            },
        )
        print(f"Glue table 'nba_players' created successfully.")
    except Exception as e:
        print(f"Error creating Glue table: {e}")

def configure_athena():
    """ Set up athena output location"""
    # query the data that is in the s3 bucket
    try:
        athena_client.start_query_execution(
            # execute sql query in athena
            QueryString="CREATE DATABASE IF NOT EXISTS nba_analytics",
            QueryExecutionContext={"Database": glue_database},
            # where to store result of the query execution
            ResultConfiguration={"OutputLocation": athena_output_location}
        )
    except Exception as e:
        print(f"Error configuring athena: {e}")

def main():
    print("Setting up a datalake for NBA sports analytics")
    create_s3_bucket()
    time.sleep(3) #ensures bucket creation propagation
    create_glue_db()
    nba_data = fetch_nba_data()
    if nba_data:
        upload_data_to_s3(nba_data)
    create_glue_table()
    configure_athena()
    print("Data lake setup complete")

if __name__ == "__main__":
    main()
