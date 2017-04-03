# Enron Email Analysis

This project aims to analyse the Enron email dataset with Spark. 

## Stage 1 - Cleaning and Ingestion - INCOMPLETE

The Enron email dataset is contained in two folders: edrm-enron-v1 and edrm-enron-v2.

edrm-enron-v2 has the data contained in xml and pst formats, along with some additional information such as document IDs etc.
We will proceed with the xml zip files only from the v2 directory.

The first stage is to unzip the files and put them into a bucket on S3 (filtering only the _xml.zip files). 
The code for this can be found in ZipExtractor.scala.

## Stage 2 - Data analysis

The analysis is carried out with a Spark job which can be found in EnronEmailAnalyser.scala.
The analysis is based on unzipped data.

### Word count

The email file content is found in the text_000 directory. The mail content is found within the .eml files.
The assumption is to count the word length of the entire files which excludes attachments but does include some metadata information.

### Top 100 recipients

The XML file in each directory contains all of the metadata for each email.
The recipient information is contained within the XPath /Root/Batch/Documents/Document/Tags in the two fields #To and #CC.

A local integration test verifies both of these analysis steps.


## Execution Instructions

Run sbt assembly to package up the application.

Create an ec2 instance and attach EBS volume snap-d203feb5

Mount the data using: 

```sudo mount /dev/xvdf /data```

Install Scala and Spark on the instance

Execute the job as follows, supplying either 'local' or 'remote' as a mode.

```spark-submit --class ZipExtractor emailIngester-assembly-1.0.jar remote``` ### This is incomplete

The next stage relies on unzipped data being present in a folder or s3 bucket.

```spark-submit --class EnronEmailAnalyser emailIngester-assembly-1.0.jar remote```