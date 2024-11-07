import oci
import os,sys

# Set your namespace and bucket details
reporting_namespace = 'bling'
prefix_file = "FOCUS Reports"  
destination_path = r'C:\Security\Blogs\Cost and Usage\Reports'

# OCI Bucket to upload the files to
dest_namespace='ociateam'
upload_bucket_name = 'cost_and_usage_reports'  # Replace with your target bucket for uploads

# Make a directory to receive reports if it doesn't exist
if not os.path.exists(destination_path):
    os.mkdir(destination_path)

# Set up OCI configuration
config = oci.config.from_file(oci.config.DEFAULT_LOCATION, oci.config.DEFAULT_PROFILE)
object_storage = oci.object_storage.ObjectStorageClient(config)
reporting_bucket = config['tenancy']
# Get the list of reports
report_bucket_objects = oci.pagination.list_call_get_all_results(object_storage.list_objects, reporting_namespace, reporting_bucket, prefix=prefix_file)

# Loop through each file in the bucket, download it, upload it to another bucket, and delete locally
for o in report_bucket_objects.data.objects:
    print(f'Found file {o.name}')
    print(o.name.rsplit('/', 1)[-1])
    sys.exit()
    
    # Download the file
    object_details = object_storage.get_object(reporting_namespace, reporting_bucket, o.name)
    filename = o.name.rsplit('/', 1)[-1]
    local_file_path = os.path.join(destination_path, filename)
    
    with open(local_file_path, 'wb') as f:
        for chunk in object_details.data.raw.stream(1024 * 1024, decode_content=False):
            f.write(chunk)
    print(f'----> File {o.name} Downloaded')

    # Upload the file to another bucket
    print(f'Uploading {filename} to bucket {upload_bucket_name}')
    with open(local_file_path, 'rb') as file_content:
        object_storage.put_object(
            namespace_name=dest_namespace,
            bucket_name=upload_bucket_name,
            object_name=filename,
            put_object_body=file_content
        )
    print(f'----> File {filename} Uploaded to {upload_bucket_name}')

    # Delete the local file after uploading
    os.remove(local_file_path)
    print(f'----> File {filename} Deleted Locally')
