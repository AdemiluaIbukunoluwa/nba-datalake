# NBA DATA-LAKE

## Overview
This system leverages Amazon S3 to store unprocessed data, creates a glue database to prepare the data for querying and configures Amazon Athea to query the data stored in the S3 bucket.

## Technologies used
- Programming Language: Python
- API: SportsData.io
- AWS Services:
    - Amazon S3: to store raw data fetched from the API
    - AWS Glue: Extracts and loads the data to prepare it for querying
    - Amazon Athena: To query data

## Architecture Diagram
![Untitled Diagram-Page-3 drawio](https://github.com/user-attachments/assets/8acf4c48-5e32-4743-b093-ee2763348c9c)


## Create .env file
This file will have: 
- your unique bucket name(S3_BUCKET_NAME)
- api key from SportsData.io(SPORTS_API_KEY)
- NBA endpoint(NBA_ENDPOINT): the api endpoint where the data is being fetched from

## PYTHON SCRIPT: src/data-lake.py./
- The python script has 6 methods
```
### create_s3_bucket:
    - Creates a bucket where the data is stored
    - The bucket name is fetched from the .env file
```
    
