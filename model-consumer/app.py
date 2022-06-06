import os

from flask import Flask, render_template, request, jsonify
from utils import log
from service import prediction_service
import pandas as pd
import logging.config

app = Flask(__name__, template_folder='templates')
log.setup_logging()
logger = logging.getLogger('modelConsumerLog')

ALLOWED_EXTENSIONS = {'csv'}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/index', methods=['GET'])
def index():
    return render_template('index.html')


@app.route('/', methods=['GET'])
def home():
    return render_template('index.html')


@app.route('/call_service', methods=['POST'])
def call_service():
    if 'file' not in request.files:
        resp = jsonify({'message': 'No file part in the request'})
        resp.status_code = 400
        return resp
    file = request.files['file']
    if 'ad_client_id' not in request.args:
        resp = jsonify({'message': 'No ad_client_id request param specified'})
        resp.status_code = 400
        return resp
    if file.filename == '':
        resp = jsonify({'message': 'No file selected for uploading'})
        resp.status_code = 400
        return resp
    if file and allowed_file(file.filename):
        df = pd.read_csv(file.stream)
        try:
            prediction_data = prediction_service.predict(df, request.args.get('ad_client_id'))
        except Exception as e:
            logger.error(e, exc_info=True)
            resp = jsonify({'message': f'{str(e)}'})
            resp.status_code = 500
            return resp
        return prediction_data.to_json(orient='records')
    else:
        resp = jsonify("Only csv files allowed")
        resp.status_code = 400
        return resp


if __name__ == '__main__':
    app.run(debug=bool(os.environ.get('FLASK_DEBUG')), host='0.0.0.0')
