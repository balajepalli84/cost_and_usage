import oci
import pandas as pd
import gzip
import io

# Initialize the OCI Object Storage client
config = oci.config.from_file()  # This assumes the default OCI config location
object_storage = oci.object_storage.ObjectStorageClient(config)

namespace_name = 'ociateam'  # Replace with your namespace
bucket_name = 'cost_and_usage_reports'

# List all objects in the bucket
object_list = object_storage.list_objects(namespace_name, bucket_name)
objects = object_list.data.objects

# Initialize an empty list to hold DataFrames
dataframes = []

# Loop over the files and load them into pandas DataFrames
for obj in objects:
    object_name = obj.name
    print(f"Processing file: {object_name}")
    
    # Get the object
    obj_data = object_storage.get_object(namespace_name, bucket_name, object_name)
    
    # Load the content of the file into a pandas DataFrame
    with gzip.GzipFile(fileobj=io.BytesIO(obj_data.data.content)) as gz:
        df = pd.read_csv(gz, low_memory=False)
        
        # Check if the required columns exist
        required_columns = ['BillingPeriodEnd', 'BillingPeriodStart', 'EffectiveCost', 'Region', 'ServiceName']
        if all(col in df.columns for col in required_columns):
            # Append DataFrame to the list
            dataframes.append(df)

# Concatenate all DataFrames into one
final_df = pd.concat(dataframes, ignore_index=True)

# Convert 'BillingPeriodStart' to datetime
final_df['BillingPeriodStart'] = pd.to_datetime(final_df['BillingPeriodStart'], utc=True, errors='coerce')

# Filter data for the last 180 days and for the service BIG_DATA in ca-toronto-1
current_time_utc = pd.Timestamp.now(tz='UTC')
last_180_days = final_df.loc[
    (final_df['BillingPeriodStart'] >= current_time_utc - pd.Timedelta(days=180)) &
    (final_df['ServiceName'] == 'BIG_DATA') &
    (final_df['Region'] == 'ca-toronto-1')
]

# Group by date and calculate the total cost for each day
daily_cost = last_180_days.groupby(final_df['BillingPeriodStart'].dt.date)['EffectiveCost'].sum().reset_index()

# Rename columns for clarity
daily_cost.columns = ['Date', 'EffectiveCost']

# Save the daily cost data to an Excel file
output_file = r'C:\Security\Blogs\Cost and Usage\Logs\big_data_ca_toronto_1_last_180_days.xlsx'
daily_cost.to_excel(output_file, index=False)

# Output file path for user
print(f"Data for BIG_DATA ca-toronto-1 for the last 180 days saved to: {output_file}")
