import io
import json
import logging
import oci
from datetime import datetime, timedelta
from fdk import response


def handler(ctx, data: io.BytesIO = None):
    name = "World"
    try:
        # Set your namespace and bucket details
        reporting_namespace = 'bling'
        yesterday = datetime.now() - timedelta(days=1)
        prefix_file = f"FOCUS Reports/{yesterday.year}/{yesterday.strftime('%m')}/{yesterday.strftime('%d')}"
        print(f"prefix is {prefix_file}")
        #prefix_file = "FOCUS Reports"  
        destination_path = '/tmp'        

        # OCI Bucket to upload the files to
        dest_namespace='ociateam'
        upload_bucket_name = 'cost_and_usage_reports'  # Replace with your target bucket for uploads

        # Set up OCI configuration
        Signer = oci.auth.signers.get_resource_principals_signer() # Get Resource Principal Credentials
        object_storage = oci.object_storage.ObjectStorageClient(config={}, signer=Signer)
        reporting_bucket = 'ocid1.tenancy.oc1..aaaaaaaaa3qmjxr43tjexx75r6gwk6vjw22ermohbw2vbxyhczksgjir7xdq'

        # Get the list of reports
        report_bucket_objects = oci.pagination.list_call_get_all_results(object_storage.list_objects, reporting_namespace, reporting_bucket, prefix=prefix_file)

        # Loop through each file in the bucket, download it, upload it to another bucket, and delete locally
        for o in report_bucket_objects.data.objects:
            print(f'Found file {o.name}',flush=True)
            
            # Download the file
            object_details = object_storage.get_object(reporting_namespace, reporting_bucket, o.name)
            filename = o.name.rsplit('/', 1)[-1]
            local_file_path = destination_path+'/'+filename
            print(f"local_file_path is  {local_file_path}")
            
            with open(local_file_path, 'wb') as f:
                for chunk in object_details.data.raw.stream(1024 * 1024, decode_content=False):
                    f.write(chunk)
            print(f'----> File {o.name} Downloaded',flush=True)

            # Upload the file to another bucket
            print(f'Uploading {filename} to bucket {upload_bucket_name}',flush=True)
            with open(local_file_path, 'rb') as file_content:
                object_storage.put_object(
                    namespace_name=dest_namespace,
                    bucket_name=upload_bucket_name,
                    object_name=filename,
                    put_object_body=file_content
                )
            print(f'----> File {filename} Uploaded to {upload_bucket_name}',flush=True)
            print(f'----> File {filename} Deleted Locally',flush=True)

    except (Exception, ValueError) as ex:
        logging.getLogger().info('error parsing json payload: ' + str(ex))
    return response.Response(
        ctx, response_data=json.dumps(
            {"message": "Processed Files sucessfully"}),
        headers={"Content-Type": "application/json"}
    )
