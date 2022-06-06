import logging
import os
import pickle
import warnings
from datetime import timedelta

import pandas
from pandas import DataFrame
from pandas.core.common import SettingWithCopyWarning
from sklearn.model_selection import GridSearchCV
from datetime import datetime
from sklearn.metrics import confusion_matrix, RocCurveDisplay
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics
import pandas as pd
from sklearn.svm import LinearSVC

from .ml_pipeline import MLPipeline

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=SettingWithCopyWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

logger = logging.getLogger('modelProducerLog')

logging.getLogger('matplotlib.font_manager').disabled = True


class ClassificationPipeline(MLPipeline):
    target = 'paid'
    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='constant', fill_value=0)),
        ('scaler', StandardScaler())])
    categorical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='constant', fill_value='Missing')),
        ('onehot', OneHotEncoder(handle_unknown='ignore', sparse=False))])

    def __init__(self, df: DataFrame, ad_client_id):
        super().__init__(ad_client_id)
        self.__features = MLPipeline.features
        self.__numeric_features = [c for c in self.__features if df[c].dtype != object]
        self.__categorical_features = [c for c in self.__features if df[c].dtype == object]
        #distribute the data evenly based on the values of the target feature so that linear models would have an even distribution of classes
        self.__data = self.distribute_data(df)

    @staticmethod
    def distribute_data(df):
        df1 = df[df.paid == 1]
        df2 = df[df.paid == 0]
        if len(df2) > len(df1):
            bigger_len = len(df2)
            lesser_len = len(df1)
        else:
            bigger_len = len(df1)
            lesser_len = len(df2)
        step = round(bigger_len / (lesser_len - 1))
        if step == 1:  # if step is 1 then the data is already evenly distributed
            ret = df
        else:
            df2ind = range(0, (lesser_len - 1) * (step + 1) + 1, step + 1)

            df1ind = [range(i + 1, i + 1 + step) for i in df2ind]
            df1ind = [i for l in df1ind for i in l][:bigger_len]

            df1.index = df1ind
            df2.index = df2ind
            ret = pandas.concat([df1, df2]).sort_index()
        del df1
        del df2
        return ret

    def run(self) -> bool:
        logger.info(f'Started classification pipeline for client: {self.ad_client_id}')

        best_params = self.evaluate_classifiers(*self.split_for_test())

        model = self.build_model(best_params, *self.split_for_train())

        self.persist_model(model)

        logger.info('Classification model persisted successfully')

        return True

    def split_for_test(self, n_years=1):
        features = self.__numeric_features + self.__categorical_features
        x_train = \
        self.__data[self.__data['dateinvoiced'] <= self.__data['dateinvoiced'].max() + timedelta(days=-365 * n_years)][
            features]
        y_train = \
        self.__data[self.__data['dateinvoiced'] <= self.__data['dateinvoiced'].max() + timedelta(days=-365 * n_years)][
            self.target]

        x_test = \
        self.__data[(self.__data['dateinvoiced'] > self.__data['dateinvoiced'].max() + timedelta(days=-365 * n_years)) &
                    (self.__data['dateinvoiced'] <= self.__data['dateinvoiced'].max() + timedelta(
                        days=-365 * (n_years - 1)))][features]
        y_test = \
        self.__data[(self.__data['dateinvoiced'] > self.__data['dateinvoiced'].max() + timedelta(days=-365 * n_years)) &
                    (self.__data['dateinvoiced'] <= self.__data['dateinvoiced'].max() + timedelta(
                        days=-365 * (n_years - 1)))][self.target]
        return x_train, y_train, x_test, y_test

    def split_for_train(self):
        df = self.__data
        features = self.__numeric_features + self.__categorical_features
        x_train = df[features]
        y_train = df[self.target]

        x_train.fillna(value=x_train.mean(), inplace=True)
        y_train.fillna(value=y_train.mean(), inplace=True)
        return x_train, y_train

    def evaluate_classifiers(self, x_train, y_train, x_test, y_test):
        param_grid = [
            {'classifier': [RandomForestClassifier()],
             'classifier__n_estimators': [10, 100, 500],
             'classifier__max_samples': [0.1, 0.2, 0.3],
             'classifier__class_weight': ['balanced']
             },
            {
                'classifier': [LinearSVC()],
                'classifier__penalty': ['l2'],
                'classifier__random_state': [0],
                'classifier__dual': [False],
                'classifier__class_weight': ['balanced'],
                'classifier__max_iter': [10000]
            }
        ]

        result_list = []

        current_best_score = 0
        current_best_params = None

        for classifier_params in param_grid:
            grid = self.train_model_grid(classifier_params, x_train, y_train)

            y_pred = grid.predict(x_test)

            display = RocCurveDisplay.from_estimator(grid.best_estimator_, x_test, y_test)
            plt = display.figure_
            if not os.path.isdir(f'client-plots/{self.ad_client_id}'):
                try:
                    os.mkdir(f'client-plots/{self.ad_client_id}')
                except OSError as error:
                    logger.error(str(error))
            current_dt = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
            plt.savefig(
                f"client-plots/{self.ad_client_id}/{str(type(classifier_params['classifier'][0]).__name__)}_{current_dt}.png")

            _confusion_matrix = metrics.confusion_matrix(y_test, y_pred)

            model_state = {
                'classifierName': type(classifier_params['classifier'][0]).__name__,
                'best_score': grid.best_score_,
                'best_params': grid.best_params_,
                'auc': metrics.roc_auc_score(y_test, y_pred),
                'precision': metrics.precision_score(y_test, y_pred),
                'recall': metrics.recall_score(y_test, y_pred),
                'f1': metrics.f1_score(y_test, y_pred),
                'FP': _confusion_matrix[0, 1],
                'FN': _confusion_matrix[1, 0],
                'TP': _confusion_matrix[1, 1],
                'TN': _confusion_matrix[0, 0],
                'grid_obj': grid
            }

            result_list.append(model_state)
            logger.info('Done with {0}'.format(type(classifier_params['classifier'][0]).__name__))
            logger.info('Grid confusion matrix: ' + str({
                'FP': _confusion_matrix[0, 1],
                'FN': _confusion_matrix[1, 0],
                'TP': _confusion_matrix[1, 1],
                'TN': _confusion_matrix[0, 0],
            }))

            if current_best_score < grid.best_score_:
                current_best_score = grid.best_score_
                current_best_params = classifier_params

        df_result = pd.DataFrame(result_list)
        df_result = df_result.sort_values(by='best_score', ascending=False)

        model = df_result.sort_values(by='best_score', ascending=False).grid_obj[0].best_estimator_

        logger.info("model score: %.3f" % model.score(x_test, y_test))

        return current_best_params

    def build_model(self, classifier_params, x_train, y_train):
        model = self.train_model_grid(classifier_params, x_train, y_train).best_estimator_
        model.fit(x_train, y_train)
        return model

    def train_model_grid(self, classifier_params, x_train, y_train):
        model = Pipeline(steps=[
            ('column_transformer', ColumnTransformer(
                transformers=[
                    ('num', self.numeric_transformer,
                     [list(x_train.columns.values).index(e) for e in self.__numeric_features]),
                    ('cat', self.categorical_transformer,
                     [list(x_train.columns.values).index(e) for e in self.__categorical_features])],
                remainder='passthrough')

             ),
            ('classifier', classifier_params['classifier'])])
        grid = GridSearchCV(model, [classifier_params], cv=5, scoring='accuracy', verbose=0, n_jobs=-1)
        # antrenare si fit
        grid.fit(x_train, y_train)
        return grid

    def persist_model(self, model):
        super().persist_model(model)
        if not os.path.isdir(f'ml-models/classification_models/{self.ad_client_id}'):
            try:
                os.mkdir(f'ml-models/classification_models/{self.ad_client_id}')
            except OSError as error:
                logger.error(str(error))
        current_dt = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
        pickle.dump(model, open(f'ml-models/classification_models/{self.ad_client_id}/{current_dt}.sav', 'wb'))