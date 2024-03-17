# data-engineering-project
Tech: AWS Glue, AWS Athena, AWS S3, AWS Lambda, Spark, Pandas
<img width="785" alt="Screenshot 2024-03-17 at 2 09 44 PM" src="https://github.com/lyhourlay1/data-engineering-project/assets/61680337/3c4e9aa0-6f2d-4324-bcd9-bac0ff6106d2">


This project:
1. download data form kaggle 
2. using aws cli to upload data into aws buckets
3. configure AWS IAM user to access buckets
4. configure crawler to generate database schema through Glue
5. configure lambda using python to pre-process data from json into parquet format by extracting s3 bucket json data and select specific data fields. Then upload the parquet file to s3 cleaned. Finally create a crawler from the cleaned s3 bucket that was generated from Lambda
6. preprocessing schema of clean database by modify the int to bigint and reran the lambda to reseed the data from (raw json bucket) into the modified schema database
7. transform cvs data from the raw database catalog into parquet format using etl job. optimize by adding partition key(seperate by region) as well as predicates(to handle certain regions)
8. create a crawler with source of cleaned csv s3 bucket(created from step 2) to generate the csv table inside the cleaned database.
9. add trigger to the previous configured lamda to insert new cleaned parquet file when someone uploads into raw s3 bucket json(raw_statistics_reference_data/). This will create a new parquet file in the cleaned s3 bucket as well as update the catalog of the clean database.
10. build etl pipeline to transfer data from clean bucket into reporting bucket to avoid using sql (athena) to manually update as this is not scalable as the data grows.

