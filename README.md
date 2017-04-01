# Enron Email Analysis

This project aims to analyse the Enron email dataset with Spark. 

## Stage 1 - Cleaning and Ingestion

The Enron email dataset is contained in two folders: edrm-enron-v1 and edrm-enron-v2.

edrm-enron-v2 has the data contained in xml and pst formats, along with some additional information such as document IDs etc.
We will proceed with the xml zip files only from the v2 directory.

The first stage is to unzip the files and put them into a new directory. 
The code for this can be found in ZipExtractor.scala

## Stage 2 - Data analysis

The Analysis is carried out with a Spark job which can be found in EnronEmailAnalyser.scala.

### Word count

The email file content is found in the text_000 directory. The mail content is found within the .eml files.
The assumption is to count the word length of the entire files which excludes attachments but does include some metadata information.

### Top 100 recipients

The XML file in each directory contains all of the metadata for each email.
The recipient information is contained within the XPath /Root/Batch/Documents/Document/Tags in the two fields #To and #CC.


## Instructions for running