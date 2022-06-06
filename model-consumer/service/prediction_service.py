import glob
import os
import pickle
from enum import Enum
from typing import List

from datasource.db import DB
from datasource.db_api import DB_Interface
from datasource.db_props import DATA_CLEANING_USER, DATA_CLEANING_PASSWORD, DATA_CLEANING_DATABASE, DATA_CLEANING_HOST, \
    DATA_CLEANING_PORT, DATA_CLEANING_SCHEMA
import pandas as pd

from pandas import DataFrame


class ModelType(Enum):
    REGRESSION = 'regression',
    CLASSIFICATION = 'classification'


features = ['closed_late_invoices_no',
            'paid_late_percent',
            'paid_late_total',
            'paid_late_raport_percent',
            'avg_days_paid_late',
            'late_unpaid_invoices_no',
            'late_unpaid_invoices_percent',
            'unpaid_invoices_late_sum',
            'late_unpaid_invoices_sum_percent'
            ] + [x + '_paid' for x in ['max', 'min', 'avg', 'std']] \
           + [x + '_late_paid' for x in ['max', 'min', 'avg', 'std']] \
           + [x + '_unpaid' for x in ['max', 'min', 'avg', 'std']] \
           + [x + '_late_unpaid' for x in ['max', 'min', 'avg', 'std']] \
           + ['late', 'dayslate', 'totalopenamt', 'paymentrule', 'tendertype']


def predict(df: DataFrame, ad_client_id):
    """
    Runs prediction for a given data frame
    adding paid_prediction column as a result of classification and daystosettle_prediction as a result of regression
    Args:
        df: Given dataframe
        ad_client_id: client for which model is run

    Returns: given dataframe

    """
    agg_data = get_aggregated_data_for_bpartners(df['c_bpartner_id'].tolist())
    if agg_data.empty:
        raise Exception('No model generated for the provided BPartners')
    joined_data = join_data(df, agg_data)
    build_derived_features(joined_data)

    classification_result = get_classification_result(joined_data, ad_client_id)
    regression_result = get_regression_result(joined_data, ad_client_id)

    df['paid_prediction'] = classification_result
    df['daystosettle_prediction'] = regression_result

    df['daystosettle_prediction'] = df[['daystosettle_prediction', 'paid_prediction']].apply(lambda x:
                                                             0 if x[1] == 0
                                                             else round(x[0], 0),
                                                             axis=1)

    return df


def get_regression_result(df: DataFrame, ad_client_id):
    __features = [f for f in features if f not in ['paid', 'daystosettle']]
    numeric_features = [c for c in __features if df[c].dtype != object]
    categorical_features = [c for c in __features if df[c].dtype == object]

    return run_model(df[numeric_features + categorical_features], ad_client_id, ModelType.REGRESSION)


def get_classification_result(df: DataFrame, ad_client_id):
    numeric_features = [c for c in features if df[c].dtype != object]
    categorical_features = [c for c in features if df[c].dtype == object]
    return run_model(df[numeric_features + categorical_features], ad_client_id, ModelType.CLASSIFICATION)


def run_model(df: DataFrame, ad_client_id: int, _type: ModelType):
    model = retrieve_model(_type, ad_client_id)
    return model.predict(df)


def retrieve_model(_type, ad_client_id):
    if _type == ModelType.REGRESSION:
        path = f'ml-models/regression_models/{ad_client_id}'
    else:
        path = f'ml-models/classification_models/{ad_client_id}'
    if not os.path.isdir(path):
        raise Exception(f"No {ModelType.REGRESSION.value[0]} model for client: {ad_client_id}")
    list_of_files = filter(os.path.isfile,
                           glob.glob(path + '/*'))
    model_file = max(list_of_files, key=os.path.getctime)
    model = pickle.load(open(model_file, 'rb'))
    return model


def build_derived_features(df: DataFrame):
    df['dateinvoiced'] = pd.to_datetime(df.dateinvoiced)
    df['duedate'] = pd.to_datetime(df.duedate)
    df['dayslate'] = (df.dateinvoiced - df.duedate).dt.days
    df['dayslate'] = df['dayslate'].apply(lambda x: 0 if x <= 0 else x)
    df['late'] = df['dayslate'].apply(lambda x: 1 if x > 0 else 0)
    df['paid'] = (df['grandtotal'] - df['paidamt']).apply(lambda x: 1 if x <= 0.01 else 0)
    df['paid_late_sum'] = df.paidamt * df.late
    df['closed_late_invoice'] = df[['late', 'paid']].apply(lambda t: 1 if t[0] == 1 and t[1] == 1 else 0, axis=1)
    df['days_late_closed_invoices_late'] = df.dayslate * df.closed_late_invoice
    df['invoice_late_unpaid'] = df.late * (1 - df.paid)
    df['unpaid_sum'] = df['grandtotal'] - df['paidamt']
    df['unpaid_late_sum'] = df['unpaid_sum'] * df.late
    df['late_unpaid_invoices_sum_percent'] = df['unpaid_invoices_late_sum'] / df['unpaid_invoices_sum']


def join_data(df_for_pred: DataFrame, agg_df: DataFrame) -> DataFrame:
    return df_for_pred.merge(agg_df, on=['ad_org_id', 'c_bpartner_id', 'c_bpartner_location_id'], how='inner')


def get_aggregated_data_for_bpartners(c_bpartner_ids: List) -> DataFrame:
    bpartner_ids_str = [str(c_bpartner_id) for c_bpartner_id in c_bpartner_ids]
    bpartner_ids_in_clause = f"({(','.join(bpartner_ids_str))})"

    db = DB(DATA_CLEANING_DATABASE,
            DATA_CLEANING_USER,
            DATA_CLEANING_PASSWORD,
            DATA_CLEANING_HOST,
            DATA_CLEANING_PORT,
            DATA_CLEANING_SCHEMA)

    db_api = DB_Interface(db)

    sql = \
        f"""
            SELECT DISTINCT ad_org_id,
               c_bpartner_id,
               c_bpartner_location_id,
               paid_late_percent,
               paid_late_raport_percent,
               closed_invoices_no,
               unpaid_invoices_no,
               late_unpaid_invoices_percent,
               closed_late_invoices_no,
               paid_total,
               paid_late_total,
               total_invoices,
               avg_days_paid_late,
               late_unpaid_invoices_no,
               unpaid_invoices_sum,
               unpaid_invoices_late_sum,
               max_paid,
               min_paid,
               avg_paid,
               std_paid,
               max_late_paid,
               min_late_paid,
               avg_late_paid,
               std_late_paid,
               max_unpaid,
               min_unpaid,
               avg_unpaid,
               std_unpaid,
               max_late_unpaid,
               min_late_unpaid,
               avg_late_unpaid,
               std_late_unpaid
            FROM {DATA_CLEANING_SCHEMA}.cleaned_aggregated_data
                    WHERE c_bpartner_id IN {bpartner_ids_in_clause} 
            """

    df = db_api.get_data_to_df(sql=sql)
    db.close()
    return df
