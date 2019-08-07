from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Start Data Quality checks')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        ## SONPLAYS TABLE
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM SONGPLAYS WHERE PLAYID IS NULL")
        if len(records) >= 1 or len(records[0]) >= 1:
            raise ValueError(f"Data quality check on table SONGPLAYS failed due to an existing PRIMARY KEY(PLAYID) with null values")
        num_records = records[0][0]
        if num_records >= 1:
            raise ValueError(f"Data quality check on table SONGPLAYS failed due to an existing PRIMARY KEY(PLAYID) with null values")
        logging.info(f"Data quality check on table SONGPLAYS have completed successfully")
        ## USERS TABLE
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM USERS WHERE USERID IS NULL")
        if len(records) >= 1 or len(records[0]) >= 1:
            raise ValueError(f"Data quality check on table USERS failed due to an existing PRIMARY KEY(USERID) with null values")
        num_records = records[0][0]
        if num_records >= 1:
            raise ValueError(f"Data quality check on table USERS failed due to an existing PRIMARY KEY(USERID) with null values")
        logging.info(f"Data quality check on table USERS have completed successfully")
        ## ARTISTS TABLE
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM ARTISTS WHERE ARTIST_ID IS NULL")
        if len(records) >= 1 or len(records[0]) >= 1:
            raise ValueError(f"Data quality check on table ARTISTS failed due to an existing PRIMARY KEY(ARTIST_ID) with null values")
        num_records = records[0][0]
        if num_records >= 1:
            raise ValueError(f"Data quality check on table ARTISTS failed due to an existing PRIMARY KEY(ARTIST_ID) with null values")
        logging.info(f"Data quality check on table ARTISTS have completed successfully")
        ## SONGS TABLE
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM SONGS WHERE SONG_ID IS NULL")
        if len(records) >= 1 or len(records[0]) >= 1:
            raise ValueError(f"Data quality check on table SONGS failed due to an existing PRIMARY KEY(SONG_ID) with null values")
        num_records = records[0][0]
        if num_records >= 1:
            raise ValueError(f"Data quality check on table SONGS failed due to an existing PRIMARY KEY(SONG_ID) with null values")
        logging.info(f"Data quality check on table SONGS have completed successfully")
        ## TIME TABLE
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM TIME WHERE START_TIME IS NULL")
        if len(records) >= 1 or len(records[0]) >= 1:
            raise ValueError(f"Data quality check on table TIME failed due to an existing PRIMARY KEY (START_TIME) with null values")
        num_records = records[0][0]
        if num_records >=1:
            raise ValueError(f"Data quality check on table TIME failed due to an existing PRIMARY KEY (START_TIME) with null values")
        logging.info(f"Data quality check on table TIME have completed successfully")