from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import pandas as pd
import numpy as np
import re
from s3fs.core import S3FileSystem

class StreetEasyOperator(BaseOperator):
    template_fields = ("s3_key", "s3_dest_key",)

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 aws_credentials_dest_id="",
                 s3_bucket="",
                 s3_dest_bucket="",
                 s3_key="",
                 s3_dest_key="",
                 *args, **kwargs):

        super(StreetEasyOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_dest_bucket = s3_dest_bucket
        self.s3_key = s3_key
        self.s3_dest_key = s3_dest_key
        self.aws_credentials_id = aws_credentials_id
        self.aws_credentials_dest_id = aws_credentials_dest_id

    def valid_searches(searches):
        # split on \\n-
        searches = searches.split('\\n-')

        # filter the list
        searches = [item for item in searches if not item.startswith('---')]

        # check if list has values
        if len(searches) == 0:
            return []
        else:
            searches = [item for item in searches if not item.startswith('---')]
            searches = [re.sub(r'(\\.n|\\.n\s+:|\\)', ' ', item) for item in searches]
            searches = [re.sub(r'\s+:', ',', item) for item in searches]
            searches = [item.split(',') for item in searches]

            # calculate valid searches
            valid_searches = []
            for item in searches:
                search_dict = {}
                for key in item:
                    if key.split(':')[0] in ('search_id', 'enabled', 'clicks', 'type', 'listings_sent', 'recommended'):
                        d_key = key.split(':')[0]
                        d_value = key.split(':')[1].strip()
                        search_dict[d_key] = d_value
                if search_dict['enabled'] == 'true' and int(search_dict.get('clicks', 0)) >= 3:
                    valid_searches.append(search_dict)
            return valid_searches

    def execute(self, context):
        self.log.info("Executing StreetEasyOperator!!")

        # get the aws hooks
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_dest_hook = AwsHook(self.aws_credentials_dest_id)

        # get the credentials for source and destination
        credentials = aws_hook.get_credentials()
        credentials_dest = aws_dest_hook.get_credentials()

        # build the s3 source path
        rendered_key = self.s3_key.format(**context)
        rendered_key_no_dashes = re.sub(r'-', '', rendered_key)
        self.log.info("Rendered Key no dashes {}".format(rendered_key_no_dashes))
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key_no_dashes)

        # get a S3 file handle
        s3 = S3FileSystem(anon=False, key=credentials.access_key, secret=credentials.secret_key)

        self.log.info("Extract data from {}".format(s3_path))
        # stream data from s3 and transform it
        with s3.open(s3_path, mode='rb') as s3_file:
            # read in the data from s3
            data = pd.read_csv(s3_file, compression='gzip', names=['user_id', 'searches'])

            # transform the data
            data['valid_searches'] = data['searches'].apply(StreetEasyOperator.valid_searches)

            data['num_valid_searches'] = data['valid_searches'].apply(len)

            # keep only searches
            data = data[data.num_valid_searches > 0]

            # remove original searches
            data = data.drop(['searches'], axis=1)

            # calculate avg_listings

            # calculate type_of_search

            self.log.info("Total valid searches today are: {}".format(np.sum(data['num_valid_searches'])))
            self.log.info("Total users today are: {}".format(np.sum(data['num_valid_searches'] > 0)))

        # build the s3 destination path
        rendered_dest_key = self.s3_dest_key.format(**context)
        rendered_dest_key_no_dashes = re.sub(r'-', '', rendered_dest_key)
        self.log.info("Rendered Key no dashes {}".format(rendered_dest_key_no_dashes))
        s3_dest_path = "s3://{}/{}".format(self.s3_dest_bucket, rendered_dest_key_no_dashes)

        # get a S3 file handle for destination
        s3_dest = S3FileSystem(anon=False, key=credentials_dest.access_key, secret=credentials_dest.secret_key)

        self.log.info("Load data into {}".format(s3_dest_path))
        # stream the transformed data into s3
        with s3_dest.open(s3_dest_path, mode='wb') as s3_dest_file:
            s3_dest_file.write(data.to_csv(None, index=False).encode())
            self.log.info("Completed writing {}".format(data.shape))

        self.log.info("ETL process completed")
