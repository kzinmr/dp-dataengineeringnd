from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries


default_args = {
    "owner": "udacity",
    "start_date": datetime(2021, 8, 9),
    "depends_on_past": False,
    "retries": 3,  # on failure, the task are retried 3 times
    "retry_delay": timedelta(minutes=5),  # retries happen every 5 minutes
    "email_on_retry": False,  # do not email on retry
    "catchup_by_default": False,  # catchup is turned off
}

dag = DAG(
    "etl_dag",
    default_args=default_args,
    description="Load JSON from S3 and transform data in Redshift.",
    schedule_interval="0 * * * *",
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table_name="public.staging_events",
    s3_bucket="s3://udacity-dend/log_data",
    auto_or_jsonpaths="s3://udacity-dend/log_json_path.json",
    execution_date="{{ execution_date }}",
    truncate_table=False,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table_name="public.staging_songs",
    s3_bucket="s3://udacity-dend/song_data",
    auto_or_jsonpaths="auto",
    execution_date="{{ execution_date }}",
    truncate_table=False,
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    table_name="public.songplays",
    insert_columns=",".join(
        [
            "playid",
            "start_time",
            "userid",
            "level",
            "songid",
            "artistid",
            "sessionid",
            "location",
            "user_agent",
        ]
    ),
    insert_select=SqlQueries.songplay_table_insert,
    truncate_table=False,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table_name="public.users",
    insert_columns=",".join(
        ["userid", "first_name", "last_name", "gender", "level"]
    ),
    insert_select=SqlQueries.user_table_insert,
    truncate_table=True,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table_name="public.songs",
    insert_columns=",".join(
        ["songid", "title", "artistid", "year", "duration"]
    ),
    insert_select=SqlQueries.song_table_insert,
    truncate_table=True,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table_name="public.artists",
    insert_columns=",".join(
        ["artistid", "name", "location", "lattitude", "longitude"]
    ),
    insert_select=SqlQueries.artist_table_insert,
    truncate_table=True,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table_name='public."time"',
    insert_columns=",".join(
        ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    ),
    insert_select=SqlQueries.time_table_insert,
    truncate_table=True,
)

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    check_queries=[
        "SELECT COUNT(*) FROM songs WHERE songid IS NULL",
        "SELECT COUNT(*) FROM songs",
        "SELECT COUNT(*) FROM songplays",
        "SELECT COUNT(*) FROM artists",
        "SELECT COUNT(*) FROM artists",
        "SELECT COUNT(*) FROM time",
    ],
    predicates=[
        lambda num_records: num_records == 0,
        lambda num_records: num_records > 0,
        lambda num_records: num_records > 0,
        lambda num_records: num_records > 0,
        lambda num_records: num_records > 0,
        lambda num_records: num_records > 0,
    ],
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
