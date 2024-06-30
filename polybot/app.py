import json

import flask
from flask import request
import os
from bot import ObjectDetectionBot
import boto3
from botocore.exceptions import ClientError

app = flask.Flask(__name__)


# TODO load TELEGRAM_TOKEN value from Secret Manager

def get_secret():
    secret_name = "Telegram-dev-token"
    region_name = "us-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name,
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    return get_secret_value_response['SecretString']


data_dict = json.loads(get_secret())
TELEGRAM_TOKEN = data_dict["TELEGRAM_TOKEN"]
TELEGRAM_APP_URL = os.environ['TELEGRAM_APP_URL']
S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']
DYNAMODB_TABLE_NAME = os.environ['DYNAMODB_TABLE_NAME']


@app.route('/', methods=['GET'])
def index():
    return 'Ok, you are connected'


@app.route(f'/{TELEGRAM_TOKEN}/', methods=['POST'])
def webhook():
    req = request.get_json()
    bot.handle_message(req['message'])
    return 'Ok'


@app.route(f'/results', methods=['POST'])
def results():
    prediction_id = request.args.get('prediction_id')

    # TODO use the prediction_id to retrieve results from DynamoDB and send to the end-user
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    response = table.get_item(Key={'prediction_id': prediction_id})
    print("THIS IS THE RESPONSE /results", response)
    if 'Item' not in response:
        return 'Prediction not found', 404

    result = response['Item']
    chat_id = result['chat_id']
    labels = result['labels']
    detected_objects = [label['class'] for label in labels]

    if detected_objects:
        detected_string = ', '.join(detected_objects)
        bot.send_text(chat_id, f"Detected objects: {detected_string}")
    else:
        bot.send_text(chat_id,"No objects detected")
    return 'Ok'


@app.route(f'/loadTest/', methods=['POST'])
def load_test():
    req = request.get_json()
    bot.handle_message(req['message'])
    return 'Ok'


if __name__ == "__main__":
    bot = ObjectDetectionBot(TELEGRAM_TOKEN, TELEGRAM_APP_URL, S3_BUCKET_NAME, SQS_QUEUE_URL)
    app.run(host='0.0.0.0', port=8443, ssl_context=('public.pem', 'private.key'))
