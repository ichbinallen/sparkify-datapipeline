# from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
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


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id='',
        table_name='',
        sql_statement='',
        *args, **kwargs
    ):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_statement = sql_statement

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Starting to load fact table %s' % self.table_name)

        sql_statement = 'INSERT INTO %s %s' % (self.table_name, self.sql_statement)

        redshift.run(sql_statement)

        self.log.info('Fact table %s load finished' % self.table_name)
