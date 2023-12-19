import boto3
import os
import time
import snappy

bucket_name = "aws-bigdata-blog"
folder_name = "artifacts/flink-refarch/data/nyc-tlc-trips.snz/"
local_directory = "/Users/rajesh/Documents/Projects/Taxi/Taxi_data"

s3_client = boto3.client('s3')

# Get a list of objects in the specified folder
objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

# Create the local directory if it doesn't already exist
if not os.path.exists(local_directory):
    os.makedirs(local_directory)

# Start broad timer
starting_broad_time = time.time()

# Download each object to the local directory
for object in objects['Contents']:
    key = object['Key']
    filename = os.path.basename(key)
    local_filename = os.path.join(local_directory, filename)

    # Check if the object is a directory
    if  object['Size'] > 0:
        print (filename, object['Size'])
        
        # Starting file download timer
        starting_file_dl_time = time.time()
        
        # Download the object to the local file
        s3_client.download_file(bucket_name, key, local_filename)
        
        # Ending file download timer
        ending_file_dl_time = time.time()
        
        # Calculation of each file download time
        duration_file_dl_time = ending_file_dl_time - starting_file_dl_time
        
        # Print duration time
        print(f"Download time: {duration_file_dl_time: .2f} seconds")
        
        # Decompress the downloaded file
        with open(local_filename, 'rb') as compressed_file:
            compressed_data = compressed_file.read()

        # Decompress the data using Snappy
        decompressed_data = snappy.decompress(compressed_data)

        # Save the decompressed data to a new file
        with open(local_filename + '.decompressed', 'wb') as decompressed_file:
            decompressed_file.write(decompressed_data)
        
# End broad timer
ending_broad_time = time.time()

# Calculation of broad download time
duration_broad_time = ending_broad_time - starting_broad_time

# Print broad duration time
print(f"It took {duration_broad_time: .2f} seconds to download and decompress all the files")      
      

        