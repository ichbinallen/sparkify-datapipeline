from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id='',
        *args, **kwargs
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = ['songplays', 'users', 'songs', 'artists', 'time']

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            recs = redshift.get_records(
                "SELECT COUNT(*) FROM {}".format(table)
            )
            if (len(recs) < 1) or (len(recs[0]) < 0):
                self.log.error("table %s is empty".format(table))
                raise ValueError(
                    "Table %s failed data quality operator".format(table)
                )
            num_records = recs[0][0]
            if num_records == 0:
                self.log.error("No records found in {}".format(table))
                raise ValueError("No records found in {}".format(table))
            pass_msg = "Data Quality check passed of table {}".format(table)
            self.log.info(pass_msg)
