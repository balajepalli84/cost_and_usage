import oci
from datetime import datetime, timedelta

# Initialize the UsageAPI client
config = oci.config.from_file()  # Make sure the ~/.oci/config file is correctly configured



# Initialize service client with default config file
usage_api_client = oci.usage_api.UsageapiClient(config)


# Send the request to service, some parameters are not required, see API
# doc for more info
request_summarized_usages_response = usage_api_client.request_summarized_usages(
    request_summarized_usages_details=oci.usage_api.models.RequestSummarizedUsagesDetails(
        tenant_id="ocid1.tenancy.oc1..aaaaaaaaa3qmjxr43tjexx75r6gwk6vjw22ermohbw2vbxyhczksgjir7xdq",
        time_usage_started=datetime.strptime(
            "2024-09-25T00:00:00.000Z",
            "%Y-%m-%dT%H:%M:%S.%fZ"),
        time_usage_ended=datetime.strptime(
            "2024-10-21T00:00:00.000Z",
            "%Y-%m-%dT%H:%M:%S.%fZ"),
        granularity="DAILY", ))

# Get the data from response
print(request_summarized_usages_response.data)