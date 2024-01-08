"""
This code will get all jobs available in the Databricks workspace and stop specified jobs on request.
"""

import requests
import json

# Set up your Databricks API endpoint and personal access token
host = 'https://<YOUR-DATABRICKS-WORKSPACE-URL>/api/2.0'
token = '<YOUR-PERSONAL-ACCESS-TOKEN>'
target_job_names = ['job_name_1', 'job_name_2']  # Add the names of the jobs you want to stop

# Headers for the API request
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

# Get all jobs
jobs_url = f'{host}/jobs/list'
response = requests.get(jobs_url, headers=headers)

if response.status_code == 200:
    jobs = response.json().get('jobs', [])
    for job in jobs:
        # Check if the job name is in the list of target job names
        if job['settings']['name'] in target_job_names:
            job_id = job['job_id']
            stop_job_url = f'{host}/jobs/runs/cancel'
            data = {
                'job_id': job_id
            }
            stop_response = requests.post(stop_job_url, headers=headers, data=json.dumps(data))
            if stop_response.status_code == 200:
                print(f"Job with ID {job_id} stopped successfully.")
            else:
                print(f"Failed to stop job with ID {job_id}.")
else:
    print("Failed to fetch jobs list.")
