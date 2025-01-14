import os
import sys
import boto3
from boto3.s3.transfer import TransferConfig
import json
import geopandas as gpd
from io import BytesIO
import threading
import time  # Import time module
from datetime import datetime
from dotenv import dotenv_values

config = TransferConfig(use_threads=False)

def start_session():
    """
    Start a new boto3 session

    Returns:
        session: boto3 session
    """
    session = boto3.Session()

    return session

def get_mfa_serial(session):
    """
    Get the multi-factor authentication (MFA) serial number from the session
    
    Args:
        session: boto3 session

    Returns:
        mfa_serial: MFA serial number
    """
    mfa_serial = session._session.full_config['profiles']['default']['mfa_serial']

    return mfa_serial

def get_mfa_token():
    """
    Get the MFA token from the user
    
    Returns:
        mfa_token: MFA token
    """
    mfa_token = input('Please enter your 6 digit MFA code:')
    
    return mfa_token

def get_session_token(session, mfa_serial=None, mfa_token=None, mfa_required=True):
    """
    Get a session token from AWS Security Token Service (STS)
    
    Args:
        session: boto3 session
        mfa_serial: MFA serial number
        mfa_token: MFA token
    
    Returns:
        MFA_validated_token: MFA validated token
    """
    sts = session.client('sts')

    if mfa_required:
        return sts.get_session_token(SerialNumber=mfa_serial, TokenCode=mfa_token)
    
    else:
        return sts.get_session_token()

def decompose_token(token):
    """
    Decompose the token into its components

    Args:
        token: MFA validated token

    Returns:
        session_token: Session token from AWS STS
        secret_access_key: Secret access key from AWS STS
        access_key_id: Access key id from AWS STS
    """
    credentials = token.get('Credentials', {})
    session_token = credentials.get('SessionToken')
    secret_access_key = credentials.get('SecretAccessKey')
    access_key_id = credentials.get('AccessKeyId')

    return session_token, secret_access_key, access_key_id

def process_message(message):
    """
    Process a message from an SQS queue

    Args:
        message: SQS message
    """
    # Process the message
    print(f"Processing message.")
    message_body = json.loads(message['Body'])
    message_body = json.loads(message_body['Message'])
    print(f"Processing message successfully completed.")

    return message_body

def poll_queue(credentials, sqs,s3):
    """
    Poll an SQS queue for messages

    Args:
        credentials: AWS credentials
        sqs: SQS client
        s3: S3 client
    """
    while True:
        response = sqs.receive_message(
            QueueUrl=credentials["sqs_url"]
        )
        if 'Messages' in response:
            for message in response['Messages']:

                # Process the message
                message_body = process_message(message)
                
                # Read the file contents
                file_contents = read_file(s3, credentials['s3_bucket'], message_body['key'])
                print(file_contents)

                # Download the file
                download_file(s3, credentials['s3_bucket'], message_body['key'], message_body['key'])

                # Delete the message from the queue
                sqs.delete_message(
                    QueueUrl=credentials["sqs_url"],
                    ReceiptHandle=message['ReceiptHandle']
                )
                
        time.sleep(1)  # Adjust the sleep time as needed

def read_file(s3, bucket, key):
    """
    Read file contents from an S3 bucket
    
    Args:
        s3: S3 client
        bucket: S3 bucket name
        key: S3 object key
    
    Returns:
        file_contents: File contents
    """
    response = s3.get_object(
        Bucket=bucket, 
        Key=key
    )
    file_contents = response['Body'].read()

    return file_contents

def download_file(s3, bucket, key, filename):
    """
    Download a file from an S3 bucket

    Args:
        s3: S3 client
        bucket: S3 bucket name
        key: S3 object key
        filename: Filename to save the file as
    """
    s3.download_file(
        Bucket=bucket,
        Key=key,
        Filename=filename,
        Config=config
    )

def main():
    # Load credentials 
    credentials = dotenv_values(".env")

    # Create a boto3 session
    session = start_session()

    # Handle SMCE MFA requirement
    mfa_serial = get_mfa_serial(session=session)
    mfa_token = get_mfa_token()
    MFA_validated_token = get_session_token(session=session, mfa_serial=mfa_serial, mfa_token=mfa_token)
    session_token, secret_access_key, access_key_id = decompose_token(MFA_validated_token)

    # # Get a session token
    # token = get_session_token(session=session, mfa_required=False)
    # session_token, secret_access_key, access_key_id = decompose_token(token)

    # Create an SQS client
    sqs = session.client('sqs', 
                aws_session_token=session_token,
                aws_secret_access_key=secret_access_key,
                aws_access_key_id=access_key_id
                )
    # Create an S3 client
    s3 = session.client('s3', 
                aws_session_token=session_token,
                aws_secret_access_key=secret_access_key,
                aws_access_key_id=access_key_id
                )

    threading.Thread(target=poll_queue, args=(credentials, sqs,s3,), daemon=True).start()

    # while True:
    #     # Get the current time
    #     current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #     print(f"Current Time: {current_time}")
        
    #     # Wait for 5 seconds
    #     time.sleep(5)

if __name__ == '__main__':
    main()