import json

import flask
from flask import request
import os
from bot import ObjectDetectionBot
import boto3
from botocore.exceptions import ClientError

app = flask.Flask(__name__)


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


def count_elements(input_list):
    element_count = {}
    for element in input_list:
        if element in element_count:
            element_count[element] += 1
        else:
            element_count[element] = 1
    return element_count


def dict_to_text(element_count):
    lines = ["I was able to recognize the following objects:"]
    for element, count in element_count.items():
        lines.append(f"{element} : {count}")
    return "\n".join(lines)


@app.route(f'/results', methods=['POST'])
def results():
    prediction_id = request.args.get('prediction_id')

    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    response = table.get_item(Key={'prediction_id': prediction_id})
    if 'Item' not in response:
        return 'Prediction not found', 404

    result = response['Item']
    chat_id = result['chat_id']
    labels = result['labels']
    detected_objects = [label['class'] for label in labels]
    detected_obj_dict = count_elements(detected_objects)

    if detected_obj_dict:
        detected_string = dict_to_text(detected_obj_dict)
        bot.send_text(chat_id, f"Detected objects: {detected_string}")
    else:
        bot.send_text(chat_id, "Sorry, I don't have my eyes today, I couldn't recognize anything")
    return 'Ok'


@app.route(f'/loadTest/', methods=['POST'])
def load_test():
    req = request.get_json()
    bot.handle_message(req['message'])
    return 'Ok'


if __name__ == "__main__":
    bot = ObjectDetectionBot(TELEGRAM_TOKEN, TELEGRAM_APP_URL, S3_BUCKET_NAME, SQS_QUEUE_URL)
    app.run(host='0.0.0.0', port=8443, ssl_context=('public.pem', 'private.key'))
