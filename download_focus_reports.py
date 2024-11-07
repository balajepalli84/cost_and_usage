import oci
import os
from datetime import datetime, timedelta

# Set your namespace and bucket details
reporting_namespace = 'bling'
prefix_file = "FOCUS Reports"
destination_path = r'C:\Security\Blogs\Cost and Usage\Reports'

# OCI Bucket to upload the files to
dest_namespace = 'ociateam'
upload_bucket_name = 'cost_and_usage_reports'

# Make a directory to receive reports if it doesn't exist
if not os.path.exists(destination_path):
    os.mkdir(destination_path)

# Set up OCI configuration
config = oci.config.from_file(oci.config.DEFAULT_LOCATION, oci.config.DEFAULT_PROFILE)
object_storage = oci.object_storage.ObjectStorageClient(config)
reporting_bucket = config['tenancy']

# Define date range for the last 10 days
current_time = datetime.now()
ten_days_ago = current_time - timedelta(days=10)

# Get the list of reports
report_bucket_objects = oci.pagination.list_call_get_all_results(
    object_storage.list_objects,
    reporting_namespace,
    reporting_bucket,
    prefix=prefix_file
)

# Loop through each file in the bucket, filter by last 10 days, download, upload, and delete locally
for o in report_bucket_objects.data.objects:
    try:
        # Parse the date from the object name (assuming format: "FOCUS Reports/YYYY/MM/DD/filename")
        _, year, month, day, filename = o.name.split('/')
        file_date = datetime(int(year), int(month), int(day))
        
        # Check if the file date is within the last 10 days
        if file_date >= ten_days_ago:
            print(f'Processing file {o.name} dated {file_date}')

            # Download the file
            object_details = object_storage.get_object(reporting_namespace, reporting_bucket, o.name)
            local_file_path = os.path.join(destination_path, filename)
            
            # Ensure subdirectories are created
            subdir_path = os.path.dirname(local_file_path)
            if not os.path.exists(subdir_path):
                os.makedirs(subdir_path)
            
            with open(local_file_path, 'wb') as f:
                for chunk in object_details.data.raw.stream(1024 * 1024, decode_content=False):
                    f.write(chunk)
            print(f'----> File {o.name} Downloaded')

            # Format the destination object name with the year, month, and day
            destination_object_name = f"FOCUS Reports/{year}/{month}/{day}/{filename}"

            # Upload the file to another bucket with the structured prefix
            print(f'Uploading {destination_object_name} to bucket {upload_bucket_name}')
            with open(local_file_path, 'rb') as file_content:
                object_storage.put_object(
                    namespace_name=dest_namespace,
                    bucket_name=upload_bucket_name,
                    object_name=destination_object_name,
                    put_object_body=file_content
                )
            print(f'----> File {destination_object_name} Uploaded to {upload_bucket_name}')

            # Delete the local file after uploading
            os.remove(local_file_path)
            print(f'----> File {filename} Deleted Locally')
        else:
            print(f'Skipping file {o.name} as it is older than 10 days')
    except ValueError as e:
        print(f"Skipping file {o.name} due to parsing error: {e}")
