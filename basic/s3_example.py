import os
import sys
import boto3
from boto3.s3.transfer import TransferConfig
import json
import geopandas as gpd
from io import BytesIO

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

def get_session_token(session, mfa_serial, mfa_token):
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

    return sts.get_session_token(SerialNumber=mfa_serial, TokenCode=mfa_token)

def decompose_mfa_validated_token(MFA_validated_token):
    """
    Decompose the MFA validated token into its components

    Args:
        MFA_validated_token: MFA validated token

    Returns:
        session_token: Session token from AWS STS
        secret_access_key: Secret access key from AWS STS
        access_key_id: Access key id from AWS STS
    """
    session_token = MFA_validated_token['Credentials']['SessionToken']
    secret_access_key = MFA_validated_token['Credentials']['SecretAccessKey']
    access_key_id = MFA_validated_token['Credentials']['AccessKeyId']

    return session_token, secret_access_key, access_key_id

def main():
    # --------------------------------------------------
    # (1) Connect to boto3 and create S3 client
    ## Create a boto3 session
    session = start_session()

    ## Handle SMCE MFA requirement
    mfa_serial = get_mfa_serial(session=session)
    mfa_token = get_mfa_token()
    MFA_validated_token = get_session_token(session=session, mfa_serial=mfa_serial, mfa_token=mfa_token)
    session_token, secret_access_key, access_key_id = decompose_mfa_validated_token(MFA_validated_token)

    ## Create an S3 client
    s3 = session.client('s3', 
                aws_session_token=session_token,
                aws_secret_access_key=secret_access_key,
                aws_access_key_id=access_key_id
                )

    # --------------------------------------------------
    # (2) List all S3 buckets
    response = s3.list_buckets()

    ## Output the bucket names
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')

    # --------------------------------------------------
    # (3) List all files
    response = s3.list_objects_v2(Bucket='snow-observing-systems')

    # Check if the bucket contains any objects
    if 'Contents' in response:
        for obj in response['Contents']:
            print(obj['Key'])
    else:
        print("Bucket is empty or does not exist.")

    # --------------------------------------------------
    # (4) Read contents of a file
    ## Get the object from the bucket
    response = s3.get_object(Bucket='snow-observing-systems', Key='Optimization_result.geojson')

    ## Read the contents of the file
    file_contents = response['Body'].read()

    ## Parse the contents as GeoJSON
    geojson_data = gpd.read_file(BytesIO(file_contents))

    # --------------------------------------------------
    # (5) Download a file
    s3.download_file('snow-observing-systems', 'Optimization_result.geojson', 'Optimization_result.geojson', Config=config)

    # --------------------------------------------------
    # (6) Upload a file
    s3.upload_file(
        's3_update.ipynb', 'snow-observing-systems', 's3_update.ipynb', Config=config
    )

if __name__ == '__main__':
    main()