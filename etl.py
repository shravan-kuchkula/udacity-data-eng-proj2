import pandas as pd
import numpy as np
import re


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

def main():
	# read in the data
	data = pd.read_csv("inferred_users.20180126.csv.gz", compression='gzip', names=['user_id', 'searches'])

	data['valid_searches'] = data['searches'].apply(valid_searches)
	data['num_valid_searches'] = data['valid_searches'].apply(len)

	print(f"Total number of valid searches today are: {np.sum(data['num_valid_searches'])}")
	print(f"Total number of users with valid searchs today are: {np.sum(data['num_valid_searches'] > 0)}")

if __name__ == '__main__':
	main()

	
