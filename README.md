# Accessing AWS S3 Buckets

This repository contains three examples: 

* [Basic](#basic): Basic access of AWS resources using the boto3 Python library.

* [MFA & non-MFA Access](#mfa--non-mfa-access): Access of AWS resources using multi-factor authentication (MFA) and non-MFA

* [Publish & Subscribe](#publish--subscribe): Application performs an action that triggers AWS resources to send a notification message. This notification message triggers another application subscribing to that queue.

## Basic

The [basic](./basic/) directory includes two files: a Jupyter notebook [s3_example.ipynb](./basic/s3_example.ipynb) and a Python script [s3_example.py](./basic/s3_example.py). Both files contain identical code, providing users with the flexibility to choose their preferred execution method.

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

<!-- 
> NOTE: This example includes the functions `get_mfa_serial()`, `get_mfa_token()`, `get_session_token()`, and `decompose_mfa_validated_token()` to facilitate multi-factor authentication (MFA) access. If MFA is not required, these functions can be omitted. -->

## MFA & non-MFA Access

The NASA Science Managed Cloud Environment (SMCE) requires MFA. However, many applications integrated within [NOS-T](https://github.com/code-lab-org/nost-tools), including the the Snow Observing Systems (SOS) applications, require continuously running applications that need not re-authenticate.

The [mfa_nonmfa](./mfa_nonmfa/) directory contains two Jupyer notebook examples, [s3_example_mfa.ipynb](./mfa_nonmfa/s3_example_mfa.ipynb) and [s3_example_nomfa.ipynb](./mfa_nonmfa/s3_example_nomfa.ipynb). These two notebooks outline the difference between MFA and non-MFA authentication within the boto3 library.

## Publish & Subscribe

The NOS-T framework employs the Advanced Message Queuing Protocol (AMQP) through RabbitMQ. However, in some scenarios, different messaging protocols are necessary. For instance, when model output data needs to be ingested by SOS applications via resources other than RabbitMQ, changes in data availability within a Simple Storage Service (S3) bucket are detected by an AWS Lambda function. This Lambda function sends a Simple Notification Service (SNS) message to a Simple Queue Service (SQS) queue. SOS applications receive data availability messages by subscribing to the SQS queue (Figure 1).

<img src="https://docs.google.com/drawings/d/e/2PACX-1vTmZIFYDTr8kw22hmZzo7mpdfYMv_oKMk9DdagOu0ESL11nvcv374iLfNZTaMVI7LT1iGR6EyGKiY7A/pub?w=1318&amp;h=764" alt="Publish/Subscribe Workflow Diagram involving AWS resources including S3, Lambda, SNS, and SQS.">

<figcaption>Figure 1: Publish/Subscribe Workflow Diagram</figcaption>

The [send_receive](./send_receive/) directory contains two Python scripts, [uploader.py](./send_receive/uploader.py) and [subscriber.py](./send_receive/subscriber.py). The `uploader.py` script uploads a file to an S3 bucket. This action triggers the Lambda function that watches for changes in data availability. The `subscriber.py` script polls the queue for any new messages. Upon receiving a data availability message, the data referenced in the message is downloaded.