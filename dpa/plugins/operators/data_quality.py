from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, db_conn_id="", dims={}, fact="", skip=False, test=None, *args, **kwargs):
        """
        Initiates the operator.
        :param db_conn_id: Connection to the Redshift database saved in Airflow.
        :param dims: Dictionary with the table_name:column_to_eval key-value pairs.
        :param fact: Name of the fact table.
        :param skip: Bool, if set to True, the Operator will be skipped.
        :param test: Reference to the function that will test the data quality.
        :param args:
        :param kwargs:
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.dims = dims
        self.fact = fact
        self.skip = skip
        self.test = test

    def execute(self, context):
        """
        Reviews the data quality of a given set of dimensions.
        :param context:
        :return:
        """
        if not self.skip:
            self.log.info("Reviewing Data Quality.")
            db = PostgresHook(self.db_conn_id)
            for dim, col in self.dims.items():
                unique_query = self.test(dim, col)
                records = db.get_records(unique_query)
                if len(records) >= 1:
                    unique, total = records[0]
                    if unique != total:
                        raise ValueError("The counts between total and unique ids are not the same.")
                    else:
                        self.log.info(f"The counts between unique {unique} and total {total} are the same."
                                      f" Data Quality check passed.")
                else:
                    raise ValueError("The query returned no values.")
        else:
            self.log.info(f"Skipping step after user selection.")