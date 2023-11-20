
import boto3 
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import argparse
  
load_dotenv()

parser = argparse.ArgumentParser()

parser.add_argument("-f", "--file", help="specify file to upload", required=True)
parser.add_argument("-b", "--bucket", help="specify aws bucket to upload to; otherwise use from environment variable BUCKET")
parser.add_argument("-k", "--keep_old", help="instruct to keep old files", action="store_true")

args = parser.parse_args()

bucket = args.bucket if args.bucket is not None else os.getenv("BUCKET")

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
)

s3 = session.resource('s3')

date = datetime.now()

filepath = args.file
file, _, file_ext = filepath.partition('.')
folder = date.strftime("%b-%Y")
filename = "{}/{}_{}.{}".format(folder, file, date.strftime("%m-%d-%y"), file_ext)

s3.meta.client.upload_file( 
    Filename=args.file, 
    Bucket=bucket,
    Key=filename
)

if not args.keep_old:
    expiration_threshold = (date - timedelta(days=365)).timestamp()
    
    response = s3.meta.client.list_objects(Bucket=bucket)

    removalQueue = []

    for object in response['Contents']:
        modified = object['LastModified']
        filename = object['Key']
        print("{} {}".format(filename, modified))
        if modified.timestamp() < expiration_threshold:
            removalQueue.append(filename)

    for file in removalQueue:
        s3.meta.client.delete_object(Bucket=bucket, Key=file)