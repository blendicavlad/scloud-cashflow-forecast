import logging

from pandas import DataFrame
import os
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder

logger = logging.getLogger('modelProducerLog')

class MLPipeline:
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

    def __init__(self, ad_client_id):
        self.ad_client_id = ad_client_id

    def persist_model(self, model):
        if not os.path.isdir('ml-models/regression_models'):
            try:
                os.mkdir('ml-models/regression_models')
            except OSError as error:
                logger.error(str(error))
        if not os.path.isdir('ml-models/classification_models'):
            try:
                os.mkdir('ml-models/classification_models')
            except OSError as error:
                logger.error(str(error))
