# etl_test

## Implementation (local env)
You will need to create a GCP Service Account and sabe the .json file with the credentials.
Build the Docker image and run de container with the following command:

`docker run -e sa_cred=/path/to/gcloud/sa/credentials -p 8080:8080 image_name:version`


## Endpoints
- Upload
- Quarterly Report
- Departments Report

## Upload File

**Example**

`curl -X POST "http://localhost:8080/upload?file_name=file_name"`

file_name: (department, hired_employees or jobs)

## Quarterly Report

**Example**

`curl -X POST "http://localhost:8080/quarterly_report?year=year"`

year= 2021

## Departments Report

**Example**

`curl -X POST "http://localhost:8080/departments_report?year=year"`

year= 2021


## Cloud implementation

I have implemented the API using Cloud Run service.
I have push the docker image to Google's Artifact Registry and I have created a Cloud Run service using the uploaded image and setting up the env variables.

