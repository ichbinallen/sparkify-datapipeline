from datetime import datetime, timedelta
# import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator
# from airflow.operators import LoadFactOperator
# from airflow.operators import LoadDimensionOperator
# from airflow.operators import DataQualityOperator
# from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


# -----------------------------------------------------------------------------
# ---- Testing Section
# -----------------------------------------------------------------------------
# from airflow.models import BaseOperator
# from airflow.utils.decorators import apply_defaults
#
#
# class LoadFactOperator(BaseOperator):
#     ui_color = '#F98866'
#     insert_statement = """
#     INSERT INTO {} ({})
#     {};
#     """
#
#     @apply_defaults
#     def __init__(self,
#                  redshift_conn_id="",
#                  table="",
#                  columns="",
#                  query="",
#                  insert_mode="append",
#                  *args, **kwargs):
#         super(LoadFactOperator, self).__init__(*args, **kwargs)
#         self.redshift_conn_id = redshift_conn_id
#         self.table = table
#         self.columns = columns
#         self.query = query
#         self.insert_mod = insert_mode
#
#     def execute(self, context):
#         self.log.info('LoadFactOperator not implemented yet')

# -----------------------------------------------------------------------------
# ---- End Testing Section
# -----------------------------------------------------------------------------


default_args = {
    'owner': 'allen',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'max_active_runs': 1,
    'retries': 0,  # set to 3 when done debuging
    'retry_delay': timedelta(minutes=0),  # 3 when done debug
    'email_on_failure': False,
    'catchup': False  # or use option in dag
    # see https://airflow.apache.org/docs/stable/scheduler.html#backfill-and-catchup
}

dag = DAG(
    'sparkify-dag',
    default_args=default_args,
    # catchup=False,
    description='Load and transform data in Redshift with Airflow'  # ,
    # schedule_interval='0 * * * *' #DONT FORGET COMMA ABOVE
)

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data"
)

# load_songplays_table = LoadFactOperator(
#     task_id='Load_songplays_fact_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     table="songplays",
#     columns="lomlan",
#     query=SqlQueries.songplay_table_insert,
#     insert_mode="append"
# )

# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag
# )
#
# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag
# )
#
# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag
# )
#
# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag
# )
#
# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag
# )
#
# end_operator = DummyOperator(
#     task_id='Stop_execution',
#     dag=dag
# )

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
# stage_events_to_redshift >> load_songplays_table
# stage_songs_to_redshift >> load_songplays_table
