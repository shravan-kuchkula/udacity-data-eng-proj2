from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import pandas as pd
import numpy as np
import re
from s3fs.core import S3FileSystem

class StreetEasyOperator(BaseOperator):
    #template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):

        super(StreetEasyOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id

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
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info("Executing StreetEasyOperator!!")
        #rendered_key = self.s3_key.format(**context)
        #s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info("Process data from {}".format(s3_path))
        s3 = S3FileSystem(anon=False, key=credentials.access_key, secret=credentials.secret_key)

        data = pd.read_csv(s3.open(s3_path, mode='rb'), compression='gzip', names=['user_id', 'searches'])
        data['valid_searches'] = data['searches'].apply(StreetEasyOperator.valid_searches)
        data['num_valid_searches'] = data['valid_searches'].apply(len)

        self.log.info("Total valid searches today are: {}".format(np.sum(data['num_valid_searches'])))
        self.log.info("Total users today are: {}".format(np.sum(data['num_valid_searches'] > 0)))
