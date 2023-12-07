from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
import os
from botocore.exceptions import NoCredentialsError 

class S3Uploader:
    def __init__(self):
        self.bucket_name = os.environ.get('bucket_name')
        self.aws_access_key_id = os.environ.get('aws_key_id')
        self.aws_secret_access_key = os.environ.get('aws_key')
        self.upload_local_file_path = os.environ.get('upload_local_file_path')
        self.s3_file_key = os.environ.get('s3_key') #this is nothing but the name of file on s3

        # set this environment variable 
            # bucket_name='s3-p-bucket'
            # aws_key_id='AKIAYHXIHF2VIAACMJ5W'
            # aws_key='ozraDYCuhDIdkWJRhjcuZKiHmOLnoF74lEI8yfK1'						                                                       upload_local_file_path='/home/chaitanya/GIT/1_GIT/SAI/sai_data.csv'
            # s3_key='sai_data.csv'
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )
        
    def upload_file(self):
        try:
            self.s3.upload_file(self.upload_local_file_path, self.bucket_name, self.s3_file_key)
            print(f"File uploaded successfully to {self.bucket_name}/{self.s3_file_key}")
        except FileNotFoundError:
            print(f"The file {self.upload_local_file_path} was not found.")
        except NoCredentialsError:
            print("Credentials not available or incorrect.")

if __name__ == '__main__':
    uploader = S3Uploader()
    uploader.upload_file()