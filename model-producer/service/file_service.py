import pickle
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

import os
import os.path
import logging
from .ml_pipeline import PipelineType

logger = logging.getLogger('modelProducerLog')

_bucket = 'ml-cashflow-forecast'


_local_models_directory = '/tmp/ml-models'

_local_plots_directory = '/tmp/client_plots'

_ml_models_bucket_key = 'client_models'

_ml_plots_bucket_key = 'client_plots'


def persist_model(pipeline_type: PipelineType, model, ad_client_id):
    if pipeline_type == PipelineType.REGRESSION:
        model_directory = 'regression_models'
    elif pipeline_type == PipelineType.CLASSIFICATION:
        model_directory = 'classification_models'
    else:
        raise Exception('Not implemented')
    model_path, obj_name = dump_model_for_client(model_directory, ad_client_id, model)
    os.chdir(os.path.abspath(os.sep))
    with open(model_path, 'rb') as f:
        if not upload_file(f.name, _ml_models_bucket_key + '/' + model_directory + '/' + str(ad_client_id), object_name=obj_name):
            raise Exception(f'Could not upload model for pipeline {pipeline_type.value[0]}')


def persist_classifier_plot(pipeline_type: PipelineType, plot, classifier_name, ad_client_id):
    if pipeline_type == PipelineType.REGRESSION:
        model_directory = 'regression_models'
    elif pipeline_type == PipelineType.CLASSIFICATION:
        model_directory = 'classification_models'
    else:
        raise Exception('Not implemented')
    plot_path, obj_name = dump_plot_for_client(ad_client_id, plot, classifier_name)
    with open(plot_path, 'rb') as f:
        if not upload_file(f.name, _ml_plots_bucket_key + '/' + model_directory + '/' + str(ad_client_id), object_name=obj_name):
            raise Exception(f'Could not upload plot for classifier {classifier_name}')


def dump_model_for_client(model_directory: str, ad_client_id, model):
    os.chdir(os.path.abspath(os.sep))
    if not os.path.isdir(f'{_local_models_directory}'):
        try:
            os.mkdir(f'{_local_models_directory}')
        except OSError as error:
            logger.error(str(error))
    if not os.path.isdir(f'{_local_models_directory}/{model_directory}'):
        try:
            os.mkdir(f'{_local_models_directory}/{model_directory}')
        except OSError as error:
            logger.error(str(error))
    if not os.path.isdir(f'{_local_models_directory}/{model_directory}/{ad_client_id}'):
        try:
            os.mkdir(f'{_local_models_directory}/{model_directory}/{ad_client_id}')
        except OSError as error:
            logger.error(str(error))
    obj_name = datetime.now().strftime("%d-%m-%Y_%H:%M:%S") + '.sav'
    model_path = f"{_local_models_directory}/{model_directory}/{ad_client_id}/{obj_name}'"
    pickle.dump(model, open(model_path, 'wb'))
    return model_path, obj_name


def dump_plot_for_client(ad_client_id, plot, classifier_name):
    os.chdir(os.path.abspath(os.sep))
    if not os.path.isdir(f'{_local_plots_directory}'):
        try:
            os.mkdir(f'{_local_plots_directory}')
        except OSError as error:
            logger.error(str(error))
    if not os.path.isdir(f'{_local_plots_directory}/{ad_client_id}'):
        try:
            os.mkdir(f'{_local_plots_directory}/{ad_client_id}')
        except OSError as error:
            logger.error(str(error))
    current_dt = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
    obj_name = f'{classifier_name}_{current_dt}.png'
    plot_path = f"{_local_plots_directory}/{ad_client_id}/{obj_name}"
    plot.savefig(plot_path)
    return plot_path, obj_name


def upload_file(file_name, key=None, bucket=_bucket, object_name=None):

    if object_name is None:
        object_name = os.path.basename(file_name)

    session = boto3.Session(
        aws_access_key_id=os.environ.get('aws_access_key_id'),
        aws_secret_access_key=os.environ.get('aws_secret_access_key'),
        aws_session_token=os.environ.get('aws_session_token')
    )
    s3_client = session.client('s3')
    # Upload the file
    try:
        if key is not None:
            _object_name = key + '/' + object_name
        else:
            _object_name = object_name
        response = s3_client.upload_file(file_name, bucket, _object_name)
    except ClientError as e:
        logger.error(e)
        return False
    return True
