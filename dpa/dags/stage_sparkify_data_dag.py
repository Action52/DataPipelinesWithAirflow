from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from dpa.plugins.operators import RedshiftStagingOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from dpa.queries import dimension_artist_create, dimension_artist_insert
from dpa.queries import dimension_song_create, dimension_song_insert
from dpa.queries import dimension_time_create, dimension_time_insert
from dpa.queries import dimension_user_create, dimension_user_insert
from dpa.queries import data_quality_check_unique

import datetime


default_args = {
    'owner': 'Luis Alfredo Leon',
    'depends_on_past': False,
    'start_date': datetime.datetime.now(),
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=3),
    'catchup': False
}

""" 
DAG DECLARATION 
"""
dag = DAG(
    "sparkify",
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1)
)

"""
STEP BEGIN EXECUTION.
"""
step_begin_execution = DummyOperator(
    task_id="begin_execution",
    dag=dag
)

"""
STAGING STEPS
"""
staging_songs = "songs_raw_data"
step_stage_songs_into_db = RedshiftStagingOperator(
    task_id="stage_songs_into_db",
    dag=dag,
    table=staging_songs,
    db_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/",
    clean=False,
    json_conf="s3://udacity-dend/log_json_path.json",
    staging_type="songs",
    skip=False
)

staging_logs = "logs_raw_data"
step_stage_logs_into_db = RedshiftStagingOperator(
    task_id="stage_logs_into_db",
    dag=dag,
    table=staging_logs,
    db_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/",
    clean=False,
    json_conf="s3://udacity-dend/log_json_path.json",
    staging_type="logs",
    skip=False
)

"""
LOAD FACT STAGE
"""
facts_table="songplays"
step_create_and_populate_facts_table = LoadFactOperator(
    task_id="create_and_populate_facts_table",
    dag=dag,
    table=facts_table,
    raw_songs_table=staging_songs,
    raw_logs_table=staging_logs,
    db_conn_id="redshift",
    skip=False
)

"""
LOAD DIMENSIONS STAGES
"""
dimension_time="dimension_time"
step_create_and_populate_dim_time = LoadDimensionOperator(
    task_id="create_and_populate_dim_time",
    dag=dag,
    table=dimension_time,
    raw_table=staging_logs,
    create_func=dimension_time_create,
    insert_func=dimension_time_insert,
    skip=False,
    db_conn_id="redshift",
    delete_first=True
)

dimension_user="dimension_user"
step_create_and_populate_dim_user = LoadDimensionOperator(
    task_id="create_and_populate_dim_user",
    dag=dag,
    table=dimension_user,
    raw_table=staging_logs,
    create_func=dimension_user_create,
    insert_func=dimension_user_insert,
    skip=False,
    db_conn_id="redshift",
    delete_first=True
)

dimension_artist="dimension_artist"
step_create_and_populate_dim_artist = LoadDimensionOperator(
    task_id="create_and_populate_dim_artist",
    dag=dag,
    table=dimension_artist,
    raw_table=staging_songs,
    create_func=dimension_artist_create,
    insert_func=dimension_artist_insert,
    delete_first=True,
    skip=False,
    db_conn_id="redshift"
)

dimension_song="dimension_song"
step_create_and_populate_dim_song = LoadDimensionOperator(
    task_id="create_and_populate_dim_song",
    dag=dag,
    table=dimension_song,
    raw_table=staging_songs,
    create_func=dimension_song_create,
    insert_func=dimension_song_insert,
    delete_first=True,
    skip=False,
    db_conn_id="redshift",
)


"""
DATA QUALITY STAGE
"""
dims = {dimension_song: "song_id",
        dimension_time: "start_time",
        dimension_user: "user_id",
        dimension_artist: "artist_id"}

step_data_quality = DataQualityOperator(
    task_id="data_quality_check",
    dag=dag,
    dims=dims,
    db_conn_id="redshift",
    test=data_quality_check_unique,
    retries=2,
    skip=False
)


"""
EXIT STAGE
"""
step_exit = DummyOperator(
    task_id="exit",
    dag=dag
)


"""
DAG ORDER DEFINITION. SHOULD BE:

                                    LOAD_DIM_SONG       -|
                STAGE_SONGS -|      LOAD_DIM_USER       -|
BEGIN_EXEC ->                    ->                         -> DATA_QUALITY_CHECK -> END_EXEC
                STAGE_LOGS  -|      LOAD_DIM_ARTIST     -|
                                    LOAD_DIM_TIME       -|

"""
step_begin_execution >> step_stage_songs_into_db >> step_create_and_populate_facts_table \
    >> step_create_and_populate_dim_artist >> step_data_quality >> step_exit
step_begin_execution >> step_stage_songs_into_db >> step_create_and_populate_facts_table \
    >> step_create_and_populate_dim_song >> step_data_quality >> step_exit
step_begin_execution >> step_stage_songs_into_db >> step_create_and_populate_facts_table \
    >> step_create_and_populate_dim_time >> step_data_quality >> step_exit
step_begin_execution >> step_stage_songs_into_db >> step_create_and_populate_facts_table \
    >> step_create_and_populate_dim_user >> step_data_quality >> step_exit

step_begin_execution >> step_stage_logs_into_db >> step_create_and_populate_facts_table \
    >> step_create_and_populate_dim_artist >> step_data_quality >> step_exit
step_begin_execution >> step_stage_logs_into_db >> step_create_and_populate_facts_table \
    >> step_create_and_populate_dim_song >> step_data_quality >> step_exit
step_begin_execution >> step_stage_logs_into_db >> step_create_and_populate_facts_table \
    >> step_create_and_populate_dim_time >> step_data_quality >> step_exit
step_begin_execution >> step_stage_logs_into_db >> step_create_and_populate_facts_table \
    >> step_create_and_populate_dim_user >> step_data_quality >> step_exit
