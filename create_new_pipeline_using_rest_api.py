import requests
import json

# Set up your Databricks API endpoint and personal access token
host = 'https://<YOUR-DATABRICKS-WORKSPACE-URL>/api/2.0'
token = '<YOUR-PERSONAL-ACCESS-TOKEN>'
notebook_path = '/path/to/your/notebook'  # Replace this with your notebook's path
pipeline_name = 'NewPipeline'  # Name for the new pipeline
schedule_interval = '0 0 * * *'  # Example: Run daily at midnight

# Headers for the API request
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

# Define the pipeline configuration
pipeline_config = {
    'name': pipeline_name,
    'objects': [
        {
            'type': 'NotebookTask',
            'notebook_path': notebook_path
        }
    ],
    'schedule': {
        'quartz_cron_expression': schedule_interval
    }
}

# Create the pipeline
create_pipeline_url = f'{host}/pipelines/create'
response = requests.post(create_pipeline_url, headers=headers, data=json.dumps(pipeline_config))

if response.status_code == 200:
    print(f"Pipeline '{pipeline_name}' created successfully!")
else:
    print("Failed to create the pipeline.")
