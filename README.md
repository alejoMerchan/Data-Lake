# Project: Data Lake

Build an ETL pipeline for a data lake hosted on S3

## High Level Functionality

In this project, we are going to use an AWS s3 and spark to build an ETL pipeline.
The project source has in S3 and they are files in JSON format, we are going to use spark to process the source data and generate a model to expose relevant business information to the project partners. 


## Deployment

1. Update the file dl.cfg with the AWS credentilas.
2. Execute the file etl.py