import requests
import json

# Set up your Databricks API endpoint and personal access token
host = 'https://<YOUR-DATABRICKS-WORKSPACE-URL>/api/2.0'
token = '<YOUR-PERSONAL-ACCESS-TOKEN>'
csv_file_path = 'dbfs:/path/to/your/csv_file.csv'  # Replace this with your CSV file path
delta_table_name = 'your_delta_table_name'  # Name for your Delta table

# Headers for the API request
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

# Define the payload to create the table
payload = {
    'tableName': delta_table_name,
    'format': 'csv',
    'path': csv_file_path,
    'options': {
        'header': 'true'  # If the CSV file has a header
    }
}

# Create the Delta table using the API
create_table_url = f'{host}/tables/create'
response = requests.post(create_table_url, headers=headers, data=json.dumps(payload))

if response.status_code == 200:
    print(f"Delta table '{delta_table_name}' created successfully!")
else:
    print("Failed to create the Delta table.")
