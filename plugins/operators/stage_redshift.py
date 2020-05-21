from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    sql_template_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        format as json {} -- use log_json_path.json in bucket root
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        self.log.info("running stagetoredshift init")
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.log.info("finished stagetoredshift init")

    def execute(self, context):
        # AWS connection
        self.log.info("testing aws conn")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info(credentials)
        self.log.info(type(credentials))

        # Redshift Conn
        self.log.info("testing redshift conn")
        self.log.info(self.redshift_conn_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        redshift.run("SELECT * FROM {} LIMIT 10".format(self.table))

    # def execute(self, context):
    #     redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

    #     self.log.info("Clearing data from destination Redshift table")
    #     redshift.run("DELETE FROM {}".format(self.table))

    #     self.log.info("Copying data from S3 to Redshift")
    #     rendered_key = self.s3_key.format(**context)
    #     s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
    #     formatted_sql = S3ToRedshiftOperator.copy_sql.format(
    #         self.table,
    #         s3_path,
    #         credentials.access_key,
    #         credentials.secret_key,
    #         self.ignore_headers,
    #         self.delimiter
    #     )
    #     redshift.run(formatted_sql)
