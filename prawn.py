import random
import os

import boto3

MAX_LABELS = 10
NUMBER_OF_SAMPLES=19
BUCKET_NAME= os.environ['BUCKET']

def get_labels(bucket, key):
    rekog = boto3.client('rekognition')
    res = rekog.detect_labels(
        Image={
            'S3Object':{
                'Bucket':bucket,
                'Name':key
            }
        },
        MaxLabels=MAX_LABELS
    )
    names = [l['Name'] for l in res['Labels']]
    if len(names)<MAX_LABELS :
        for i in range(MAX_LABELS -len(names)):
            names.append("null")
    return names

def get_csv_headers(n):
    headers = ''
    for i in range(n):
        headers+='rekog{},'.format(i)
    headers += 'isprwn\n'
    return headers

def rekog_prawns():
    row_list = []
    for i in range(NUMBER_OF_SAMPLES):
        key = 'examples/prawn{}.jpg'.format(i)
        print(key)
        res = get_labels(BUCKET_NAME, key)
        res.append('1')
        row_list.append(res)
    return row_list

def rekog_not_prawns():
    row_list = []
    for i in range(NUMBER_OF_SAMPLES):
        key = 'examples/notprawn{}.jpg'.format(i)
        print(key)
        res = get_labels(BUCKET_NAME, key)
        res.append('0')
        row_list.append(res)
    return row_list

def generate_training_csv():
    row_list = rekog_prawns() + rekog_not_prawns()
    random.shuffle(row_list)
    csv = get_csv_headers(MAX_LABELS)
    for row in row_list:
        row_string = ','.join(row) +'\n'
        csv += row_string
    return csv

def put_training_data_to_s3(raw):
    s3 = boto3.client('s3')
    s3.put_object(
        ACL='private',
        Bucket=BUCKET_NAME,
        Body=raw,
        Key='training.csv'
    )
    print('Put to s3')

if __name__ == '__main__' :
    put_training_data_to_s3(generate_training_csv())
