# Youtube Data Analysis
> A company wants to run an advertisement campaign, and they've selected Youtube as their main channel. However, they want to understand current trends in Youtube data before commencing.

## Overview
This aim of the project is to manage, streamline, and analyse structured and semi-structured Youtube data in a secure manner, based on various video categories and trending metrics.

## Project goals
1. Data ingestion - Developing a mechanism to ingest data from various sources
2. ETL - Obtaining data in raw format, transforming it into a proper format
3. Reporting - Building a dashboard to identify current trends in Youtube data
4. Cloud computing - Understanding and utilising various cloud-based services to process large amounts of data

## Services used
1. AWS CLI - A unified command line interface allowing users to manage AWS services from the terminal
2. AWS IAM - A service that allows users to securely control access to AWS users
3. Amazon S3 - An object storage service that offers scalability, data availability, security, and performance
4. AWS Glue - A serverless data integration service that makes it easier to discover, prepare, move, and integrate data from multiple sources for analytics, Machine Learning (ML), and application development
5. AWS Athena - An interactive query service that makes it simple to analyse data stored in S3, using standard SQL
6. AWS Lambda - A serverless compute service that allows users to run code without having to worry about running servers
7. AWS Quicksight - A fully managed, serverless Business Intelligence (BI) service for the AWS cloud

## Setting up IAM access
Firstly, I created an IAM user to build the project through, as opposed to using the root account (the account you set up using your e-mail). I also attached existing permissions to the user for each of the services used as the project progressed.

## Setting up AWS CLI
I installed AWS CLI from the AWS website. Next, I used the `aws configure` command in the terminal, providing the access key and secret access key to set up the IAM user on the CLI, as well as further information such as the region.

## Creating an initial S3 bucket
I created an initial S3 bucket to store the raw data in, naming it "youtube-raw-data-useast1-dev". The data utilised in the project consists of both the raw data (csv) and the corresponding reference data (json). After adding the files to the directory, I then used the following command: `aws s3 cp . s3://youtube-raw-data-useast1-dev/youtube/raw_statistics_reference_data/ --recursive --exclude "*" --include "*.json"` to copy the reference data to the s3 bucket; to copy the actual data, I used the following command, replacing the region, which is 'ca' in this case, for each country: `aws s3 cp CAvideos.csv s3://youtube-raw-data-useast1-dev/youtube/raw_statistics/region=ca/`. This ensured that each csv file would get stored in an individual folder corresponding to each country. 

## Creating the Glue catalog, and querying the data with AWS Athena and SQL
After creating the bucket, I now had to create the AWS Glue catalog. This included creating a crawler, which I called "youtube-raw-data-glue-catalog-1", to access the data in the S3 bucket (the reference data in particular), extract the metadata, and create table definitions in the Glue catalog. While creating the crawler, I had to create a new database in which the raw data would be stored, which I called youtube_raw. After the crawler had successfully run, a table was now created, which contained the reference data.

When opening up Athena, I was prompted to add an S3 bucket where the query results would be stored; I called mine 'youtube-raw-data-useast1-athena-job'. I then attempted to query the data, however, this was unsuccessful due to the way in which the reference data was formatted in each JSON file, and as a result, Glue was unable to read it.

## Building an AWS Lambda function to convert the JSON data to Parquet
As a result of this error, I built a Lambda function to convert the reference data to Parquet format, due to its row-column structure, making it more computer-readable. Initially, I performed a test run on the code by sampling a json file corresponding to the US. After initially getting an error due to the AWS Wrangler module not being present, I added an AWS layer, which then allowed the AWS Wrangler module to be imported. 

When trying to run the script again, I then received a timeout error, however, after incresing the memory allocated to run the function, it ran successfully, creating and storing the US reference data in parquet format, in an S3 bucket corresponding to cleansed data, which I called 'youtube-cleansed-data-useast1-dev', while also creating a new database called "db_youtube_cleaned" and table containing this information within the Glue catalog. This time, I was able to query the data successfully using Athena.

## Building a crawler to access the actual data
After transforming the reference data, it was now time to work on the actual data. I built and ran another crawler, called 'youtube-raw-data-csv-crawler', that would crawl through the actual data, which was in csv format, and add the data to the 'youtube_raw' database by creating another table called 'raw_statistics', while also partitioning the data based on the region. Eventually, after making changes to the schema of the 'raw_statistics' table, deleting the parquet file corresponding to the US reference data, and re-testing the lambda function, the following SQL query ran successfully:

```sql 
SELECT a.title, a.category_id, b.snippet_title FROM "youtube_raw"."raw_statistics" as a
INNER JOIN "db_youtube_cleaned"."cleaned_statistics_reference_data" as b
ON a.category_id = b.id
WHERE a.region = 'us';
```

## Adding an ETL job through AWS Glue
Next, I built the first ETL pipeline, which transforms the csv data to parquet format, and stores it in the 'youtube-cleansed-data-useast1-dev' S3 bucket, partitioning each file based on region. For the sake of this project, I applied a filter to only work with the data corresponding to the UK, USA, and Canada, due to the encoding required to work with the data corresponding to some of the other countries, such as Japan and Korea. The code can be found in the [pyspark_code.py](https://github.com/HarshShah2812/youtube-data-analysis/blob/main/pyspark_code.py) file above.

## Crawling the actual data again and adding a trigger to the Lambda function
I created another crawler, called 'youtube-cleaned-data-csv-to-parquet-etl', to access the actual data in the "youtube_raw" database, and store the resulting data in the "db_youtube_cleaned" database. 

In order to automate the transformation of the json files to parquet, I added an S3 trigger to the Lambda function. To test this trigger, I deleted the existing files from the S3 bucket, and copied the json files in the local directory to the S3 bucket using AWS CLI, with the result being that the files were automatically transformed to parquet and stored in the "youtube-cleansed-data-useast1-dev" bucket.

## Building the final ETL pipeline
Using AWS Glue, I built the final ETL job, calling it 'youtube-parquet-analytics-version', to prepare the cleaned data for analysis. This job would read both the actual and reference data stored in the 'db_youtube_cleaned' database, join them, and write the joined frame to another S3 bucket, which I called 'youtube-analytics-data-useast1-dev', to store the analytics-ready data. The code can be found in the [parquet_analytics_etl_job.py](https://github.com/HarshShah2812/youtube-data-analysis/blob/main/parquet_analytics_etl_job.py) file above.

The resulting folders in the S3 bucket looks something like this:

<img width="1089" alt="Screenshot 2024-01-06 at 16 37 18" src="https://github.com/HarshShah2812/youtube-data-analysis/assets/67421468/fc633f05-60df-4493-8845-14efa1bcfdf0">

<img width="1089" alt="Screenshot 2024-01-06 at 12 04 34" src="https://github.com/HarshShah2812/youtube-data-analysis/assets/67421468/71a01f7a-7d00-447d-aa1b-b869344a59cf">