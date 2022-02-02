from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries


class RedshiftStagingOperator(BaseOperator):
    ui_color = '#38f5d9'

    @apply_defaults
    def __init__(self, db_conn_id="", aws_credentials_id="", table="", s3_bucket="", s3_key="", delimiter="",
                 ignore_headers=1, clean=False, staging_type="", json_conf="", skip=False,*args, **kwargs):
        """
        Initiates the operator.
        :param db_conn_id: Connection to the Redshift database saved in Airflow.
        :param aws_credentials_id: Connection to the aws credentials saved in Airflow.
        :param table: Table name.
        :param s3_bucket: Bucket to download the raw data.
        :param s3_key: s3 Key from which download the raw data.
        :param delimiter: Delimiter to use.
        :param ignore_headers: If passed, states if we want to ignore the headers.
        :param clean: Bool, determines if we shall clean the data from a previously existing table.
        :param staging_type: songs or logs
        :param json_conf: If we are staging the logs data, we have to pass the json map for this data. Full s3 path.
        :param skip: Skips the step if passed.
        :param args:
        :param kwargs:
        """
        super(RedshiftStagingOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.clean = clean
        self.staging_type = staging_type
        self.json_conf = json_conf
        self.skip = skip

    def execute(self, context):
        """
        Ingests the data from s3 into Redshift.
        :param context:
        :return:
        """
        if not self.skip:
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()
            db = PostgresHook(self.db_conn_id)
            self.log.info("Creating table if not exists.")
            if self.staging_type == "songs":
                create_query = SqlQueries.staging_songs_table_create(self.table)
            else:
                create_query = SqlQueries.staging_logs_table_create(self.table)
            db.run(create_query)
            if self.clean:
                self.log.info("Cleaning table.")
                clean_query = self.clean_table()
                db.run(clean_query)
            self.log.info("Streaming data from s3 to db.")
            rendered_key = self.s3_key.format(**context)
            s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
            if self.staging_type == "songs":
                copy_query = self.copy_table_songs(copy_table=self.table, source=s3_path)
            else:
                copy_query = self.copy_table_logs(copy_table=self.table, source=s3_path, json_conf=self.json_conf)
            db.run(copy_query)
            self.log.info(f"Data copied successfully into {self.table}")
        else:
            self.log.info(f"Skipping step after user selection.")

    def clean_table(self):
        return f"DELETE FROM {self.table};"

    @staticmethod
    def copy_table_songs(copy_table="", source=""):
        arn = Variable.get('redshift_iam_arn')
        return SqlQueries.staging_songs_table_copy(copy_table, source, arn)

    @staticmethod
    def copy_table_logs(copy_table="", source="", json_conf=""):
        arn = Variable.get('redshift_iam_arn')
        return SqlQueries.staging_logs_table_copy(copy_table, source, arn, json_conf)
