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
I installed AWS CLI from the AWS website. Next, I used the `aws configure` command in the terminal, providing the access key and secret access key to set up the IAM user on the CLI, as well as further information such as a the region.



