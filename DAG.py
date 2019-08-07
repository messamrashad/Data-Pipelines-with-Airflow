from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'catchup': False,
    'retry_delay': timedelta(seconds=300),
    'retries': '3',
    'depends_on_past': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval = "@hourly"
          #schedule_interval = '0 0 * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = "staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "log_data/{execution_date.year}/{execution_date.month}",
    path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = "staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "song-data/A/A",
    path="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    query = SqlQueries.songplay_table_insert,
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials"
)
    
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    query = SqlQueries.user_table_insert,
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials",
    operation="insert",
    table="users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    query = SqlQueries.song_table_insert,
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials",
    operation="insert",
    table="songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    query = SqlQueries.artist_table_insert,
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials",
    operation="insert",
    table="artists"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    query = SqlQueries.time_table_insert,
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials",
    operation="insert",
    table="time"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator