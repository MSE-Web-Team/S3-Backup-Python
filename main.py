#!/bin/python3

import boto3 
import os
import math
import sys
from dotenv import load_dotenv
from datetime import datetime, timedelta
import argparse
from pathlib import Path
from tqdm import tqdm
import warnings

warnings.filterwarnings('ignore')

sys.path.append('./cron-job-tracker')

import job as cron_job_tracker
  
load_dotenv()

API_PATH = os.getenv("DASHBOARD_URL")

def getCurrentDatetime():
    return datetime.now()

def getS3Client():
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
    )

    return session.resource('s3')

def getArgs():
    parser = argparse.ArgumentParser()

    parser.add_argument("-f", "--file", help="specify file to upload", required=True)
    parser.add_argument("-b", "--bucket", help="specify aws bucket to upload to", required=True)
    parser.add_argument("-k", "--keep_old", help="instruct to keep old files", action="store_true")
    parser.add_argument("-d", "--dir", help="generate a directory to store file in", action='store_true')
    parser.add_argument("-r", "--report_progress", help="report progress to dashboard")

    return parser.parse_args()

def generateUploadedFilename(args):
    date = getCurrentDatetime()

    filepath = Path(args.file)

    folder = date.strftime("%b-%Y")

    filename = "{}_{}".format(date.strftime("%m-%d-%y"), filepath.name)

    if args.dir:
        filename = "{}/{}_{}".format(folder, date.strftime("%m-%d-%y"), filepath.name)

    return filename

def deleteOldFiles(s3, args):
    if not args.keep_old:
        expiration_threshold = (getCurrentDatetime() - timedelta(days=32)).timestamp()

        response = s3.meta.client.list_objects(Bucket=args.bucket)

        removalQueue = []

        for object in response['Contents']:
            modified = object['LastModified']
            filename = object['Key']

            if modified.timestamp() < expiration_threshold:
                removalQueue.append(filename)

        for file in removalQueue:
            s3.meta.client.delete_object(Bucket=args.bucket, Key=file)

def startCronJob(report_progress):
    if report_progress and len(report_progress) > 0:
        cron_job_tracker.create_job(report_progress, API_PATH)

def updateCronJob(report_progress, status = "SUCCESS"):
    if report_progress and len(report_progress) > 0:
        cron_job_tracker.update_job_status(report_progress, API_PATH, status)

def uploadFile(s3, args):
    #init multipart upload
    filename = generateUploadedFilename(args)
    mp = s3.meta.client.create_multipart_upload(
        Bucket=args.bucket,
        Key=filename
    )

    upload_id = mp['UploadId']
    
    filesize = os.stat(args.file).st_size # in bytes
    file_chunk_size = (1024**2) * 50 #50MB chunks
    chunks_count = int(math.ceil(filesize / float(file_chunk_size)))

    parts = []

    with open(args.file, 'rb') as f:
        for i in tqdm(range(chunks_count)):
            offset = file_chunk_size * i
            bytes = min(file_chunk_size, filesize - offset)
            
            f.seek(offset)
            part = f.read(bytes)
            
            mp = s3.meta.client.upload_part(
                Body=part,
                Bucket=args.bucket,
                Key=filename,
                PartNumber=i+1,
                UploadId=upload_id
            )

            parts.append({'PartNumber':i+1, 'ETag': mp['ETag']})
    
    s3.meta.client.complete_multipart_upload(
        Bucket=args.bucket,
        Key=filename,
        UploadId=upload_id,
        MultipartUpload={ 'Parts': parts }
    )


args = getArgs()

s3 = getS3Client()

startCronJob(args.report_progress)

try:
    uploadFile(s3, args)

    deleteOldFiles(s3, args)

    updateCronJob(args.report_progress)
except:
    print(f"Upload failed")
    updateCronJob(args.report_progress, status = "ERROR")