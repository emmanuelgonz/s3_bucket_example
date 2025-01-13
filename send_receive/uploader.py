import os
import sys
import boto3
from boto3.s3.transfer import TransferConfig
import json
import geopandas as gpd
from io import BytesIO
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

    # Create an S3 client
    s3 = session.client('s3', 
                aws_session_token=session_token,
                aws_secret_access_key=secret_access_key,
                aws_access_key_id=access_key_id
                )

    # Upload data
    s3.upload_file(
        'Optimization_result.geojson', credentials['s3_bucket'], 'Optimization_result.geojson', Config=config
    )

    # Get the current time
    current_time = datetime.now()

    # Print the current time
    print(f'Data uploaded successfully at {current_time}!')

if __name__ == '__main__':
    main()