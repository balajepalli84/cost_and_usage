import oci
import pandas as pd
import gzip
import io
from scipy import stats
import numpy as np

# Initialize the OCI Object Storage client
config = oci.config.from_file()  # This assumes the default OCI config location
object_storage = oci.object_storage.ObjectStorageClient(config)

namespace_name = 'ociateam'  # Replace with your namespace
bucket_name = 'cost_and_usage_reports'

# List all objects in the bucket (no need to filter by prefix)
object_list = object_storage.list_objects(namespace_name, bucket_name)
objects = object_list.data.objects

# Initialize an empty list to hold DataFrames and track files without required columns
dataframes = []
missing_column_files = []

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
        else:
            # Log missing column file
            missing_column_files.append(object_name)

# Print names of files missing required columns
if missing_column_files:
    print("Files missing required columns:")
    for file_name in missing_column_files:
        print(file_name)

# Proceed if valid dataframes are present
if dataframes:
    # Concatenate all DataFrames into one
    final_df = pd.concat(dataframes, ignore_index=True)

    # Convert 'BillingPeriodStart' and 'BillingPeriodEnd' to datetime
    final_df['BillingPeriodStart'] = pd.to_datetime(final_df['BillingPeriodStart'], utc=True, errors='coerce')
    final_df['BillingPeriodEnd'] = pd.to_datetime(final_df['BillingPeriodEnd'], utc=True, errors='coerce')

    # Filter data for the last 120 days
    current_time_utc = pd.Timestamp.now(tz='UTC')
    last_120_days = final_df[final_df['BillingPeriodStart'] >= current_time_utc - pd.Timedelta(days=120)]

    # Group by service and region, and sum 'EffectiveCost'
    grouped_data = last_120_days.groupby(['ServiceName', 'Region'])['EffectiveCost'].sum().reset_index()

    # Calculate z-scores for anomaly detection
    grouped_data['z_score'] = stats.zscore(grouped_data['EffectiveCost'])

    # Find anomalies based on z-score threshold
    anomalies = grouped_data[np.abs(grouped_data['z_score']) > 3]

    # Prepare to add historical cost data next to each anomaly
    historical_summaries = []

    # For each anomaly, extract the historical monthly EffectiveCost
    for index, row in anomalies.iterrows():
        service = row['ServiceName']
        region = row['Region']
        
        # Filter historical data for the outlier service-region combination
        historical_data = final_df[(final_df['ServiceName'] == service) & (final_df['Region'] == region)]
        
        # Group by month and calculate the total cost for each month
        historical_data['month'] = historical_data['BillingPeriodStart'].dt.to_period('M')
        monthly_cost = historical_data.groupby('month')['EffectiveCost'].sum().reset_index()
        
        # Create a summary string of the monthly cost, e.g., "Apr: $90, May: $100, Jun: $300"
        monthly_summary = ', '.join(f"{row['month'].strftime('%b')}: ${row['EffectiveCost']:.2f}" for _, row in monthly_cost.iterrows())
        
        # Append the summary to the list
        historical_summaries.append(monthly_summary)
    
    # Add the historical summary as a new column to the anomalies DataFrame
    anomalies['Historical Monthly Cost'] = historical_summaries

    # Save anomalies to a CSV file with the historical cost included
    anomaly_output_file = r'C:\Security\Blogs\Cost and Usage\Logs\usage_anomalies_with_history.csv'
    anomalies.to_csv(anomaly_output_file, index=False)

    # Output file path for user
    print(f"Anomalies with historical data saved to: {anomaly_output_file}")
else:
    print("No valid data found with required columns.")
