# dp-dataengineeringnd
In this project, we try to build analytics data pipeline for a Music service. We perform an ETL pipeline using Airflow.

## Setup Airflow
- Make a user that has AmazonRedshiftFullAccess and AmazonS3ReadOnlyAccess policies.
- Set plugins and dags folders in our Airflow folder.
- Add two connections in Airflow Admin > Connections:
  - redshift: connection type: PostgreSQL with environment and credential information.
  - aws_credentials: connection type: Amazon Web Services, with user credentials to access S3.

## How to run

- Run /opt/airflow/start.sh command to start the Airflow web server.
- Trigger tasks in Airflow UI.
- Make sure that start_date and schedule_interval in the DAG are properly configured.

## Dags outline

Add default parameters according to these guidelines:

- The DAG does not have dependencies on past runs
- On failure, the task are retried 3 times
- Retries happen every 5 minutes
- Catchup is turned off
- Do not email on retry

The graph view of our dags and dependencies follows the flow shown in the image below:

![example-dat.png](https://github.com/kzinmr/dp-dataengineeringnd/blob/75c3f68693d832409bc5ce8b09ed09a54038deaa/example-dag.png "DAG with task dependencies")


## Operators outline

- Stage Operator
  - Load JSON files from S3 to Amazon Redshift.
  - Make SQL COPY based on given parameters in which the source JSON and the target tables are specified.
  - Add a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.
- Fact and Dimension Operators
  - Transform data with the provided SQL helpers.
  - Take as input a SQL statement and target database and define a target table.
  - Add a parameter that allows switching between insert modes.
    - Dimension should be loaded with truncate-insert pattern where the
    target table is emptied before the load.
    - Fact tables should allow only append type functionality because
    they are usually so massive.
- Data Quality Operator
  - receive one or more SQL based test cases along with the expected results
  - raise exceptions and retry task if there is no match
