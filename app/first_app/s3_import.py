from chat_postgres import save_messages
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

def upload_file_to_s3(local_file_path, bucket_name, s3_object_key):
    # Initialize the S3 client
    s3_client = boto3.client('s3')
    
    try:
        # Upload the file
        s3_client.upload_file(local_file_path, bucket_name, s3_object_key)
        print(f"Successfully uploaded {local_file_path} to {bucket_name}/{s3_object_key}")
    except FileNotFoundError:
        print("The system could not find the local file specified.")
    except NoCredentialsError:
        print("AWS credentials not found. Please run 'aws configure'.")
    except ClientError as e:
        print(f"An error occurred: {e}")

# Usage Example
upload_file_to_s3('my_document.pdf', 'my-example-bucket-name', 'documents/my_document.pdf')
