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
    Takes data from S3, calculates search stats, uploads it to Reshift table.

    :param aws_credentials_id: reference to source aws hook containing iam details.
    :type aws_credentials_id: str
    :param redshift_conn_id: reference to a specific redshift cluster hook
    :type redshift_conn_id: str
    :param table: destination table on redshift.
    :type table: str
    :param columns: columns of the destination table
    :type columns: str containing column names in csv format.
    :param s3_bucket: source s3 bucket name
    :type s3_bucket: str
    :param s3_key: source s3 file (templated)
    :type s3_key: Can receive a str representing a prefix,
        the prefix can contain a path that is partitioned by some field.
    :param today: date of execution (templated)
    :type today: Can receive a str representing the execution_date.
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

        # put the columns in the format INSERT table expect
        columns = "({})".format(self.columns)

        # build the s3 source path
        # as we are providing_context = True, we get them in kwargs form
        # use **context to upack the dictionary and format the s3_key
        rendered_key = self.s3_key.format(**context)
        rendered_key_no_dashes = re.sub(r'-', '', rendered_key)
        self.log.info("Rendered Key no dashes {}".format(rendered_key_no_dashes))
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key_no_dashes)

        # get a S3 file handle and pass in the creds
        s3 = S3FileSystem(anon=False, key=credentials.access_key, secret=credentials.secret_key)

        # stream data from s3
        # we don't want to store a local copy of the file on airflow worker's disk
        # thus, we are processing this in-memory using with-open-file construct.
        with s3.open(s3_path, mode='rb') as s3_file:
            # read in the data from s3
            data = pd.read_csv(s3_file)
            self.log.info("Shape of the data is {}".format(data.shape))

            # as we are providing_context = True, we get them in kwargs form
            # use **context to upack the dictionary and format the today ivar
            render_today = self.today.format(**context)
            self.log.info("Today is {}".format(render_today))

            # calculate summary stats
            num_valid_searches = np.sum(data['num_valid_searches'])
            num_users_with_valid_searches = np.sum(data['num_valid_searches'] > 0)
            num_rental_searches = np.sum(data['type_of_search'] == 'rental')
            num_sales_searches = np.sum(data['type_of_search'] == 'sale')
            num_rental_and_sales_searches = np.sum(data['type_of_search'] == 'rental_and_sale')
            num_none_type_searches = np.sum(data['type_of_search'] == 'none')

            # prepare values to be sent to INSERT stmt
            values = (render_today, num_valid_searches, num_users_with_valid_searches,
                        num_rental_searches, num_sales_searches, num_rental_and_sales_searches,
                        num_none_type_searches)

            self.log.info("Loading stats into Redshift table")
            self.log.info("Total valid searches today are: {}".format(np.sum(data['num_valid_searches'])))
            self.log.info("Total users today are: {}".format(np.sum(data['num_valid_searches'] > 0)))
            self.log.info("Total rental searches today are: {}".format(np.sum(data['type_of_search'] == 'rental')))
            self.log.info("Total sales searches today are: {}".format(np.sum(data['type_of_search'] == 'sale')))
            self.log.info("Total rental and sales searches today are: {}".format(np.sum(data['type_of_search'] == 'rental_and_sale')))
            self.log.info("Total none type searches today are: {}".format(np.sum(data['type_of_search'] == 'none')))

            # build the insert statement
            load_sql = ValidSearchStatsOperator.load_search_stats_sql.format(
                self.table,
                columns,
                values
                )

            # load data into redshift
            redshift_hook.run(load_sql)
