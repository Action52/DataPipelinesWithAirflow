from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    @apply_defaults
    def __init__(self, db_conn_id="", aws_credentials_id="", table="", raw_table="", create_func=None,
                 insert_func=None, skip=False, delete_first=False, *args, **kwargs):
        """
        Initiates the operator.
        :param db_conn_id: Connection to the Redshift database saved in Airflow.
        :param aws_credentials_id: Reference to the aws credentials saved in Airflow.
        :param table: Table name for the dimension.
        :param raw_table: Table name for the raw_table data (songs or logs).
        :param create_func: Reference to the function that creates the table.
        :param insert_func: Reference to the function that inserts the data into the table.
        :param skip: Bool, if set to True, the operator will skip.
        :param delete_first: Bool, if set to True, the operator will delete first the table.
        :param args:
        :param kwargs:
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.raw_table = raw_table
        self.skip = skip
        self.create_func = create_func
        self.insert_func = insert_func
        self.delete_first = delete_first

    def execute(self, context):
        """
        Handles the data loading of the dimensions.
        :param context:
        :return:
        """
        if not self.skip:
            db = PostgresHook(self.db_conn_id)
            if self.delete_first:
                self.log.info(f"Deleting table {self.table}.")
                delete_query = SqlQueries.delete_table(self.table)
                db.run(delete_query)
                self.log.info(f"Creating table {self.table}.")
                create_query = self.create_func(self.table)
                db.run(create_query)
            self.log.info(f"Inserting data into dimension {self.table}.")
            insert_query = self.insert_func(self.table, self.raw_table)
            db.run(insert_query)
        else:
            self.log.info(f"Skipping step after user selection.")