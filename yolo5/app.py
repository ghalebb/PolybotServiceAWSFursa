import time
from decimal import Decimal
from pathlib import Path
from detect import run
import yaml
from loguru import logger
import os
import boto3
import requests
# from flask import Flask, jsonify

# Flask app for health check
# app = Flask(__name__)
images_bucket = os.environ['BUCKET_NAME']
queue_name = os.environ['SQS_QUEUE_NAME']
region_name = os.environ['REGION_NAME']
dynamodb_table_name = os.environ['DYNAMODB_TABLE_NAME']
polybot_endpoint = os.environ['POLYBOT_ENDPOINT']

sqs_client = boto3.client('sqs', region_name=region_name)
s3_client = boto3.client('s3', region_name=region_name)
dynamodb = boto3.resource('dynamodb', region_name=region_name)
table = dynamodb.Table(dynamodb_table_name)

with open("data/coco128.yaml", "r") as stream:
    names = yaml.safe_load(stream)['names']


def download_image_from_s3(bucket, img_name, local_path):
    s3_client.download_file(bucket, img_name, local_path)


def upload_image_to_s3(bucket, local_path, s3_path):
    s3_client.upload_file(local_path, bucket, s3_path)


def convert_to_decimal(data):
    if isinstance(data, list):
        return [convert_to_decimal(i) for i in data]
    elif isinstance(data, dict):
        return {k: convert_to_decimal(v) for k, v in data.items()}
    elif isinstance(data, float):
        return Decimal(str(data))
    else:
        return data


def consume():
    while True:
        response = sqs_client.receive_message(QueueUrl=queue_name, MaxNumberOfMessages=1, WaitTimeSeconds=5)

        if 'Messages' in response:
            message = response['Messages'][0]['Body']
            receipt_handle = response['Messages'][0]['ReceiptHandle']
            prediction_id = response['Messages'][0]['MessageId']

            logger.info(f'prediction: {prediction_id}. start processing')

            # Extract img_name and chat_id from the message
            msg_data = yaml.safe_load(message)
            img_name = msg_data['s3_key']
            chat_id = msg_data['chat_id']
            original_img_path = f"/tmp/{img_name}"

            # Download the image from S3
            download_image_from_s3(images_bucket, img_name, original_img_path)
            logger.info(f'prediction: {prediction_id}/{original_img_path}. Download img completed')

            # Predict objects in the image
            run(
                weights='yolov5s.pt',
                data='data/coco128.yaml',
                source=original_img_path,
                project='static/data',
                name=prediction_id,
                save_txt=True
            )
            logger.info(f'prediction: {prediction_id}/{original_img_path}. done')

            predicted_img_path = f'static/data/{prediction_id}/{img_name}'

            # Upload the predicted image to S3
            predicted_s3_path = f'predicted/{img_name}'
            upload_image_to_s3(images_bucket, predicted_img_path, predicted_s3_path)

            # Parse prediction labels and create a summary
            pred_summary_path = Path(f'static/data/{prediction_id}/labels/{img_name.split(".")[0]}.txt')
            if pred_summary_path.exists():
                with open(pred_summary_path) as f:
                    labels = f.read().splitlines()
                    labels = [line.split(' ') for line in labels]
                    labels = [{
                        'class': names[int(l[0])],
                        'cx': convert_to_decimal(float(l[1])),
                        'cy': convert_to_decimal(float(l[2])),
                        'width': convert_to_decimal(float(l[3])),
                        'height': convert_to_decimal(float(l[4])),
                    } for l in labels]

                logger.info(f'prediction: {prediction_id}/{original_img_path}. prediction summary:\n\n{labels}')

                prediction_summary = {
                    'prediction_id': prediction_id,
                    'original_img_path': original_img_path,
                    'predicted_img_path': predicted_s3_path,
                    'labels': labels,
                    'time': convert_to_decimal(time.time()),
                    'chat_id': chat_id
                }

                # Store the prediction_summary in a DynamoDB table
                table.put_item(Item=prediction_summary)

                # Perform a GET request to Polybot to `/results` endpoint
                response = requests.get(f'{polybot_endpoint}/results',
                                         params={'prediction_id': prediction_id, 'chat_id': chat_id})
                if response.status_code == 200:
                    logger.info(f'prediction: {prediction_id}. Polybot results endpoint notified successfully')
                else:
                    logger.error(f'prediction: {prediction_id}. Failed to notify Polybot results endpoint')

            # Delete the message from the queue as the job is considered as DONE
            sqs_client.delete_message(QueueUrl=queue_name, ReceiptHandle=receipt_handle)


if __name__ == "__main__":
    consume()
