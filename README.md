# Accessing AWS S3 Buckets

This repository includes two files: a Jupyter notebook (s3_example.ipynb) and a Python script (s3_example.py). Both files contain identical code, providing users with the flexibility to choose their preferred execution method.

Below are the sections covered in the example:

1. Connect to boto3 and create S3 client
    * Demonstrates how to establish a connection to AWS using the [boto3 library](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) and create an S3 client, which is essential for interacting with S3 buckets
1. List all S3 buckets
    * Retrieve and display a list of all S3 buckets in your AWS account, helping you manage and organize your storage resources
1. List all files
    * List all the files (objects) within a specific S3 bucket, providing an overview of the contents stored in the bucket
1. Read contents of a file
    * Read and display the contents of a file stored in an S3 bucket, which is useful for accessing and processing data directly from S3
1. Download a file
    * Download a file from an S3 bucket to your local machine, allowing you to work with the file offline or integrate it into other applications
1. Upload a file
    * Upload a file from your local machine to an S3 bucket, enabling you to store and share data securely in the cloud

> NOTE: This example includes the functions `get_mfa_serial()`, `get_mfa_token()`, `get_session_token()`, and `decompose_mfa_validated_token()` to facilitate MFA access. If MFA is not required, these functions can be omitted.
