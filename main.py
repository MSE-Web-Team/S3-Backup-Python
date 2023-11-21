#!/bin/python

import boto3 
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import argparse
from pathlib import Path
  
load_dotenv()

parser = argparse.ArgumentParser()

parser.add_argument("-f", "--file", help="specify file to upload", required=True)
parser.add_argument("-b", "--bucket", help="specify aws bucket to upload to", required=True)
parser.add_argument("-k", "--keep_old", help="instruct to keep old files", action="store_true")
parser.add_argument("-d", "--dir", help="generate a directory to store file in")

args = parser.parse_args()

bucket = args.bucket

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
)

s3 = session.resource('s3')

date = datetime.now()

filepath = Path(args.file)

folder = date.strftime("%b-%Y")

filename = "{}_{}".format(date.strftime("%m-%d-%y"), filepath.name)

if args.dir:
    filename = "{}/{}_{}".format(folder, date.strftime("%m-%d-%y"), filepath.name)

s3.meta.client.upload_file(
    Filename=args.file,
    Bucket=bucket,
    Key=filename
)

if not args.keep_old:
    expiration_threshold = (date - timedelta(days=32)).timestamp()
    
    response = s3.meta.client.list_objects(Bucket=bucket)

    removalQueue = []

    for object in response['Contents']:
        modified = object['LastModified']
        filename = object['Key']
        #print("{} {}".format(filename, modified))
        if modified.timestamp() < expiration_threshold:
            removalQueue.append(filename)

    for file in removalQueue:
        s3.meta.client.delete_object(Bucket=bucket, Key=file)