from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.transforms import valid_searches
from helpers.transforms import avg_listings_sent
from helpers.transforms import type_of_search
from helpers.transforms import list_of_valid_searches

import pandas as pd
import numpy as np
import re
from s3fs.core import S3FileSystem

class StreetEasyOperator(BaseOperator):
    """
    Extract data from source S3, process it in-memory, load it to dest S3.

    :param aws_credentials_id: reference to source aws hook containing iam details.
    :type aws_credentials_id: str
    :param aws_credentials_dest_id: reference to dest aws hook containing iam details.
    :type aws_credentials_id: str
    :param s3_bucket: source s3 bucket name
    :type s3_bucket: str
    :param s3_dest_bucket: destination s3 bucket name
    :type s3_dest_bucket: str
    :param s3_key: source s3 file (templated)
    :type s3_key: Can receive a str representing a prefix,
        the prefix can contain a path that is partitioned by some field.
    :param s3_dest_key: first destination s3 file (templated)
    :type s3_dest_key: Can receive a str representing a prefix,
        the prefix can contain a path that is partitioned by some field.
    :param s3_dest_df_key: second destination s3 file (templated)
    :type s3_dest_df_key: Can receive a str representing a prefix,
        the prefix can contain a path that is partitioned by some field.
    """
    template_fields = ("s3_key", "s3_dest_key", "s3_dest_df_key",)

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 aws_credentials_dest_id="",
                 s3_bucket="",
                 s3_dest_bucket="",
                 s3_key="",
                 s3_dest_key="",
                 s3_dest_df_key="",
                 *args, **kwargs):

        super(StreetEasyOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.aws_credentials_dest_id = aws_credentials_dest_id
        self.s3_bucket = s3_bucket
        self.s3_dest_bucket = s3_dest_bucket
        self.s3_key = s3_key
        self.s3_dest_key = s3_dest_key
        self.s3_dest_df_key = s3_dest_df_key


    def execute(self, context):
        self.log.info("Executing StreetEasyOperator!!")

        # get the aws hooks
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_dest_hook = AwsHook(self.aws_credentials_dest_id)

        # get the credentials for source and destination
        credentials = aws_hook.get_credentials()
        credentials_dest = aws_dest_hook.get_credentials()

        # build the s3 source path
        # as we are providing_context = True, we get them in kwargs form
        # use **context to upack the dictionary and format the s3_key
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

            # create valid searches
            data['valid_searches'] = data['searches'].apply(valid_searches)

            # calculate num valid searches per user
            data['num_valid_searches'] = data['valid_searches'].apply(len)

            # keep only valid searches
            data = data[data.num_valid_searches > 0].reset_index(drop=True)

            # remove original searches
            data = data.drop(['searches'], axis=1)

            # calculate avg_listings_sent
            data['avg_listings'] = data['valid_searches'].apply(avg_listings_sent)

            # calculate type_of_search
            data['type_of_search'] = data['valid_searches'].apply(type_of_search)

            # prepare a list of valid search ids
            data['list_of_valid_searches'] = data['valid_searches'].apply(list_of_valid_searches)

            # drop valid searches as we don't need it anymore
            data = data.drop(['valid_searches'], axis=1)

            # get unique valid searches
            unique_valid_searches = set()
            for sublist in data['list_of_valid_searches']:
                for item in sublist:
                    unique_valid_searches.add(re.sub(r'\'', '', item))

            # construct a dataframe
            unique_valid_searches_df = pd.DataFrame({'searches': list(unique_valid_searches)})

            self.log.info("Total valid searches today are: {}".format(np.sum(data['num_valid_searches'])))
            self.log.info("Total users today are: {}".format(np.sum(data['num_valid_searches'] > 0)))

        # build the s3 destination path
        rendered_dest_key = self.s3_dest_key.format(**context)
        rendered_dest_key_no_dashes = re.sub(r'-', '', rendered_dest_key)
        self.log.info("Rendered Key no dashes {}".format(rendered_dest_key_no_dashes))
        s3_dest_path = "s3://{}/{}".format(self.s3_dest_bucket, rendered_dest_key_no_dashes)

        # get a S3 file handle for destination
        s3_dest = S3FileSystem(anon=False, key=credentials_dest.access_key, secret=credentials_dest.secret_key)

        # stream the transformed data into s3
        with s3_dest.open(s3_dest_path, mode='wb') as s3_dest_file:
            self.log.info("Started writing {}".format(unique_valid_searches_df.shape))
            s3_dest_file.write(unique_valid_searches_df.to_csv(None, index=False).encode())
            self.log.info("Completed writing {}".format(unique_valid_searches_df.shape))

        rendered_dest_df_key = self.s3_dest_df_key.format(**context)
        rendered_dest_df_key_no_dashes = re.sub(r'-', '', rendered_dest_df_key)
        self.log.info("Rendered Key no dashes {}".format(rendered_dest_df_key_no_dashes))
        s3_dest_df_path = "s3://{}/{}".format(self.s3_dest_bucket, rendered_dest_df_key_no_dashes)

        with s3_dest.open(s3_dest_df_path, mode='wb') as s3_dest_file:
            self.log.info("Started writing {}".format(data.shape))
            s3_dest_file.write(data.to_csv(None, index=False).encode())
            self.log.info("Completed writing {}".format(data.shape))

        self.log.info("StreetEasyOperator completed")
