from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from dpa.queries import songplays_table_create, songplays_table_insert


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, db_conn_id="", aws_credentials_id="", table="", raw_songs_table="", raw_logs_table="",
                 skip=False, *args, **kwargs):
        """
        Initiates the operator.
        :param db_conn_id: Connection to the Redshift database saved in Airflow.
        :param aws_credentials_id: Connection to the aws credentials saved in Airflow.
        :param table: Fact table name.
        :param raw_songs_table: Table name for the raw songs table.
        :param raw_logs_table: Table name for the raw logs table.
        :param skip: Bool, skips if set to True.
        :param args:
        :param kwargs:
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.raw_songs_table = raw_songs_table
        self.raw_logs_table = raw_logs_table
        self.skip = skip

    def execute(self, context):
        """
        Executes the data loading step for the facts table.
        :param context:
        :return:
        """
        if not self.skip:
            db = PostgresHook(self.db_conn_id)
            self.log.info(f"Creating table {self.table}.")
            create_query = songplays_table_create(self.table)
            db.run(create_query)
            self.log.info(f"Inserting data into facts table.")
            insert_query = songplays_table_insert(self.table, self.raw_songs_table, self.raw_logs_table)
            db.run(insert_query)
        else:
            self.log.info(f"Skipping step after user selection.")
