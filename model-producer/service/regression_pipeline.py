import logging
import numpy as np
from pandas.core.common import SettingWithCopyWarning
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer

from sklearn.linear_model import Lasso, LassoCV
from sklearn.metrics import mean_squared_error
from datetime import timedelta
from pandas import DataFrame
from sentry_sdk import start_span
import time
import datetime

from . import file_service
from .ml_pipeline import MLPipeline, PipelineType
from .classification_pipeline import ClassificationPipeline
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=SettingWithCopyWarning)

logger = logging.getLogger('modelProducerLog')


class RegressionPipeline(MLPipeline):
    target = 'daystosettle'

    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='constant', fill_value=0))])

    categorical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='constant', fill_value='Missing')),
        ('onehot', OneHotEncoder(handle_unknown='ignore', sparse=False))])

    def __init__(self, df: DataFrame, ad_client_id: int):
        super().__init__(ad_client_id)
        self.__features = [f for f in MLPipeline.features if
                           f not in [ClassificationPipeline.target, RegressionPipeline.target]]
        self.__numeric_features = [c for c in self.__features if df[c].dtype != object]
        self.__categorical_features = [c for c in self.__features if df[c].dtype == object]
        self.__cols_reg = self.__numeric_features + self.__categorical_features + [self.target, self.target,
                                                                                   'dateinvoiced']
        self.__data = df

    def run(self) -> bool:
        logger.info(f'Started regression pipeline for client: {self.ad_client_id}')
        x_train, y_train, x_test, y_test = self.split_for_test()
        with start_span(op="regression_evaluation", description="Regression evaluation") as span:
            start_time = time.time()
            model_for_test = self.build_model(x_train, y_train)
            span.set_data('seconds_run', str(datetime.timedelta(seconds=(time.time() - start_time))))
            self.test_model(model_for_test, x_train, y_train, x_test, y_test)
            span.set_data('ad_client_id', self.ad_client_id)

        with start_span(op="regression_model_building", description="Regression model building") as span:
            start_time = time.time()
            generated_model = self.build_model(*self.split_for_train())
            self.persist_model(generated_model)
            logger.info('Regression model persisted successfully')
            span.set_data('seconds_run', str(datetime.timedelta(seconds=(time.time() - start_time))))
            span.set_data('ad_client_id', self.ad_client_id)

        return True

    def split_for_test(self, n_years=1):
        df = self.__data
        features = self.__numeric_features + self.__categorical_features
        x_train = df[(df.paid == 1) &
                     (df['dateinvoiced'] <= df['dateinvoiced'].max() + timedelta(days=-365 * n_years))][
            features]
        y_train = df[(df.paid == 1) &
                     (df['dateinvoiced'] <= df['dateinvoiced'].max() + timedelta(days=-365 * n_years))][self.target]

        x_test = \
            df[(df.paid == 1) & (df['dateinvoiced'] > df['dateinvoiced'].max() + timedelta(days=-365 * n_years)) &
               (df['dateinvoiced'] <= df['dateinvoiced'].max() + timedelta(days=-365 * (n_years - 1)))][features]

        y_test = \
            df[(df.paid == 1) & (df['dateinvoiced'] > df['dateinvoiced'].max() + timedelta(days=-365 * n_years)) &
               (df['dateinvoiced'] <= df['dateinvoiced'].max() + timedelta(days=-365 * (n_years - 1)))][self.target]

        x_train.fillna(value=x_train.mean(), inplace=True)
        y_train.fillna(value=y_train.mean(), inplace=True)
        x_test.fillna(value=x_test.mean(), inplace=True)
        y_test.fillna(value=y_test.mean(), inplace=True)
        return x_train, y_train, x_test, y_test

    def split_for_train(self):
        df = self.__data
        features = self.__numeric_features + self.__categorical_features
        x_train = df[(df.paid == 1)][features]
        y_train = df[(df.paid == 1)][self.target]

        x_train.fillna(value=x_train.mean(), inplace=True)
        y_train.fillna(value=y_train.mean(), inplace=True)
        return x_train, y_train

    @staticmethod
    def test_model(model, x_train, y_train, x_test, y_test):
        y_test_reg = model.predict(x_train)

        cv_alpha_test_error = mean_squared_error(y_train,
                                                 y_test_reg)

        logger.info("MSE of test data :" + str(cv_alpha_test_error))
        logger.info("R^2 of test data: {0}".format(model.score(x_test, y_test)))

    def build_model(self, x_train, y_train):
        df = self.__data[self.__cols_reg].reset_index()
        cols = [c for c in df if
                c in self.__numeric_features + self.__categorical_features + [RegressionPipeline.target]]

        # lasso are un CV propriu. fapt pentru care standardizarea se va face acolo si o scoatem din preprocesare
        numeric_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='constant', fill_value=0))])

        categorical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='constant', fill_value='Missing')),
            ('onehot', OneHotEncoder(handle_unknown='ignore', sparse=False))])

        data_transformer = Pipeline(steps=[
            ('column_transformer', ColumnTransformer(
                transformers=[
                    ('num', numeric_transformer,
                     [list(df[cols].columns.values).index(e) for e in self.__numeric_features]),
                    ('cat', categorical_transformer,
                     [list(df[cols].columns.values).index(e) for e in self.__categorical_features])],
                remainder='passthrough'
            )
             )])

        lambda_values = 10 ** np.linspace(10, -3, 100) * 0.5
        x_train_transform = data_transformer.fit_transform(x_train)

        lassocv = LassoCV(alphas=lambda_values, cv=10, max_iter=100000, normalize=True)

        # antrenare si fit
        lassocv.fit(x_train_transform, y_train)

        # pas 5
        lambda_optim = lassocv.alpha_

        # pas 6: reantrenare pe toate datele de antrenament
        lasso = Lasso(max_iter=100000, normalize=True)
        lasso.set_params(alpha=lambda_optim)
        lasso.fit(x_train_transform, y_train)

        # pas 7 - raportatea erorii

        model = Pipeline(steps=[
            ('column_transformer', ColumnTransformer(
                transformers=[
                    ('num', self.numeric_transformer,
                     [list(df[cols].columns.values).index(e) for e in self.__numeric_features]),
                    ('cat', self.categorical_transformer,
                     [list(df[cols].columns.values).index(e) for e in self.__categorical_features])],
                remainder='passthrough')),
            ('lasso', lasso)
        ])

        model.fit(x_train, y_train)

        logger.info("R^2 of train data: {0}".format(model.score(x_train, y_train)))

        del df

        return model

    def persist_model(self, model):
        file_service.persist_model(PipelineType.REGRESSION, model, self.ad_client_id)
