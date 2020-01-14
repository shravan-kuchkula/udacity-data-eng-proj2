from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import pandas as pd
import numpy as np
import re
from s3fs.core import S3FileSystem

class ValidSearchStatsOperator(BaseOperator):
    """
    Loads data to the given table by running the provided sql statement.

    :param redshift_conn_id: reference to a specific redshift cluster hook
    :type redshift_conn_id: str
    :param table: destination fact table on redshift.
    :type table: str
    :param columns: columns of the destination fact table
    :type columns: str containing column names in csv format.
    :param sql_stmt: sql statement to be executed.
    :type sql_stmt: str
    :param append: if False, a delete-insert is performed.
        if True, a append is performed.
        (default value: False)
    :type append: bool
    """
    ui_color = '#80BD9E'
    template_fields = ("s3_key", "today", )
    load_search_stats_sql = """
        INSERT INTO {} {} VALUES {}
    """
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 columns="",
                 s3_bucket="",
                 s3_key="",
                 today="",
                 *args, **kwargs):

        super(ValidSearchStatsOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.columns = columns
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.today = today

    def execute(self, context):
        self.log.info('ValidSearchStatsOperator has started')
        # get the hooks
        redshift_hook = PostgresHook(self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)

        # get the credentials for s3
        credentials = aws_hook.get_credentials()

        columns = "({})".format(self.columns)

        # build the s3 source path
        rendered_key = self.s3_key.format(**context)
        rendered_key_no_dashes = re.sub(r'-', '', rendered_key)
        self.log.info("Rendered Key no dashes {}".format(rendered_key_no_dashes))
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key_no_dashes)

        # get a S3 file handle
        s3 = S3FileSystem(anon=False, key=credentials.access_key, secret=credentials.secret_key)

        self.log.info("Extract data from {}".format(s3_path))
        # stream data from s3
        with s3.open(s3_path, mode='rb') as s3_file:
            # read in the data from s3
            data = pd.read_csv(s3_file)
            self.log.info("Shape of the data is {}".format(data.shape))

            render_today = self.today.format(**context)
            self.log.info("Today is {}".format(render_today))

            num_valid_searches = np.sum(data['num_valid_searches'])
            num_users_with_valid_searches = np.sum(data['num_valid_searches'] > 0)
            values = (render_today, num_valid_searches, num_users_with_valid_searches)

            self.log.info("Total valid searches today are: {}".format(np.sum(data['num_valid_searches'])))
            self.log.info("Total users today are: {}".format(np.sum(data['num_valid_searches'] > 0)))

            load_sql = ValidSearchStatsOperator.load_search_stats_sql.format(
                self.table,
                columns,
                values
                )

            redshift_hook.run(load_sql)
