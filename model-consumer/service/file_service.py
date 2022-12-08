import pickle

import boto3
from botocore.exceptions import ClientError
from .model_type import ModelType

import os
import os.path
import logging

logger = logging.getLogger('modelProducerLog')

_bucket = 'ml-cashflow-forecast'

_local_models_directory = '/tmp/ml-models'

_ml_models_bucket_key = 'client_models'


def retrieve_model(ad_client_id, model_type: ModelType):
    s3_model_path = get_most_recent_model_for_client(ad_client_id, model_type)
    if s3_model_path is None:
        raise Exception(f"No {model_type.value[0]} model for client: {ad_client_id}")

    local_file_path = _local_models_directory + '/' + s3_model_path['Key'][s3_model_path['Key'].rfind('/') + 1:]
    os.chdir(os.path.abspath(os.sep))
    create_local_tmp_directories()
    download_file(bucket=_bucket, object_name=s3_model_path['Key'], file_name=local_file_path)
    model = pickle.load(open(local_file_path, 'rb'))
    return model


def create_local_tmp_directories():
    if not os.path.isdir(f'{_local_models_directory}'):
        try:
            os.mkdir(f'{_local_models_directory}')
        except OSError as error:
            logger.error(str(error))


def get_most_recent_model_for_client(ad_client_id, model_type: ModelType, bucket_name=_bucket):
    if model_type == ModelType.REGRESSION:
        model_directory = 'regression_models'
    elif model_type == ModelType.CLASSIFICATION:
        model_directory = 'classification_models'
    else:
        raise Exception('Not implemented')
    session = boto3.Session(
        aws_access_key_id=os.environ.get('aws_access_key_id'),
        aws_secret_access_key=os.environ.get('aws_secret_access_key'),
        aws_session_token=os.environ.get('aws_session_token')
    )
    s3_client = session.client('s3')
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name,
                                       Prefix=f'{_ml_models_bucket_key}/{model_directory}/{ad_client_id}')
    latest = None
    for page in page_iterator:
        if "Contents" in page:
            latest2 = max(page['Contents'], key=lambda x: x['LastModified'])
            if latest is None or latest2['LastModified'] > latest['LastModified']:
                latest = latest2
    return latest


def download_file(file_name, key=None, bucket=_bucket, object_name=None):
    """Download a file from an S3 bucket
    :param file_name: File to upload
    :param key: Key of bucket
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False

    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    session = boto3.Session(
        aws_access_key_id=os.environ.get('aws_access_key_id'),
        aws_secret_access_key=os.environ.get('aws_secret_access_key'),
        aws_session_token=os.environ.get('aws_session_token')
    )
    s3_client = session.client('s3')

    try:
        if key is not None:
            _object_name = key + '/' + object_name
        else:
            _object_name = object_name
            s3_client.download_file(bucket, _object_name, file_name)
    except ClientError as e:
        logger.error(e)
        return False
    return True
