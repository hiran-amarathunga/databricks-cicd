import requests

# Set up your Databricks API endpoint and personal access token
host = 'https://<YOUR-DATABRICKS-WORKSPACE-URL>/api/2.0'
token = '<YOUR-PERSONAL-ACCESS-TOKEN>'

# List of job names to start
jobs_to_start = ['job_name_1', 'job_name_2', 'job_name_3']  # Add the names of the jobs you want to start

# Headers for the API request
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

# Fetch all jobs to map names to IDs
jobs_url = f'{host}/jobs/list'
response = requests.get(jobs_url, headers=headers)

if response.status_code == 200:
    jobs = response.json().get('jobs', [])
    job_name_to_id = {job['settings']['name']: job['job_id'] for job in jobs}

    for job_name in jobs_to_start:
        if job_name in job_name_to_id:
            job_id = job_name_to_id[job_name]
            runs_url = f'{host}/jobs/runs/create'
            data = {
                'job_id': job_id
            }
            response = requests.post(runs_url, headers=headers, json=data)
            
            if response.status_code == 200:
                run_id = response.json().get('run_id')
                print(f"Job '{job_name}' started. Run ID: {run_id}")
            else:
                print(f"Failed to start job '{job_name}'. Status code: {response.status_code}")
        else:
            print(f"Job '{job_name}' not found.")
else:
    print("Failed to fetch jobs list.")
