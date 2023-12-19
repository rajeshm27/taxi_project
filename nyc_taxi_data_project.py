import boto3
import time
import bz2
from datetime import datetime
import json
from io import BytesIO
import argparse

def list_objects(bucket_name, folder_name):
    s3_client = boto3.client('s3')
    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name, Delimiter='/')
    return objects['Contents']

def process_object(s3_client, bucket_name, key):
    # Starting file download timer
    starting_file_dl_time = time.time()

    # Download the object to memory
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    content = response['Body'].read()

    # Ending file download timer
    ending_file_dl_time = time.time()

    # Calculation of each file download time
    duration_file_dl_time = ending_file_dl_time - starting_file_dl_time

    # Print duration time
    print(f"Download time: {duration_file_dl_time:.2f} seconds")

    with bz2.open(BytesIO(content), 'rb') as file:
        # Read the decompressed data
        data = file.read()

        # Now you can work with the data
        # If it's text data, you might want to decode it
        text_data = data.decode('utf-8')
        
        # Count the number of JSON records (assuming each line is a record)
        json_records = text_data.count('\n')
        
    # Print the number of JSON records
    print(f"Number of JSON records: {json_records}")

    # Process each line in the decompressed file
    earliest_date, latest_date = process_events(text_data)

    return earliest_date, latest_date

def process_events(text_data):
    earliest_date = None
    latest_date = None

    # Process each line in the decompressed file
    for line in text_data.split('\n'):
        # Assuming each line is a JSON record with a date field
        try:
            json_record = json.loads(line)
            record_date = datetime.strptime(json_record['pickup_datetime'], "%Y-%m-%dT%H:%M:%S.%fZ")
            if earliest_date is None or record_date < earliest_date:
                earliest_date = record_date
            if latest_date is None or record_date > latest_date:
                latest_date = record_date
        except (json.JSONDecodeError, KeyError, ValueError):
            # Ignore lines without a valid JSON format or missing date field
            pass

    return earliest_date, latest_date

def main():
    parser = argparse.ArgumentParser(description='taxi_project') 
    parser.add_argument('--bucket_name', help='AWS S3 bucket name')
    parser.add_argument('--folder_name', help='AWS S3 folder name')
    args = parser.parse_args()
    
    bucket_name = args.bucket_name
    folder_name = args.folder_name

    s3_client = boto3.client('s3')

    # Start broad timer
    starting_broad_time = time.time()

    # Variables to store earliest and latest event dates
    earliest_date = None
    latest_date = None

    # Get a list of objects in the specified folder
    objects = list_objects(bucket_name, folder_name)
    
    # Determine the number of objects
    number_of_objects = len(objects)
    print(f"Number of objects: {number_of_objects}")
    
    # Download each object to memory and process
    for object in objects:
        key = object['Key']

        # Check if the object is a directory
        if object['Size'] > 0:
            # Print filename and file size explicitly
            print(f"File: {key}, File Size: {object['Size']} bytes")

            earliest, latest = process_object(s3_client, bucket_name, key)

            if earliest and latest:
                if earliest_date is None or earliest < earliest_date:
                    earliest_date = earliest
                if latest_date is None or latest > latest_date:
                    latest_date = latest

    # End broad timer
    ending_broad_time = time.time()

    # Print earliest and latest event dates
    if earliest_date and latest_date:
        print(f"Earliest Event Date: {earliest_date}")
        print(f"Latest Event Date: {latest_date}")

    # Calculation of broad download time
    duration_broad_time = ending_broad_time - starting_broad_time

    # Print broad duration time
    print(f"It took {duration_broad_time:.2f} seconds to download & decompress all the files")

if __name__ == "__main__":
    main()
