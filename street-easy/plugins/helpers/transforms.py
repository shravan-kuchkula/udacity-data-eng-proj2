import re
import pandas as pd
import numpy as np

def valid_searches(searches):
    ''' Parses the search string and returns only valid searches.
        Additional details: intended to be applied to a pandas series.

        :param searches: raw unparsed search string
        :type str
        :return valid_searches: list of valid searches
        :type list
    '''
    # each search is delimited by \\n-
    searches = searches.split('\\n-')

    # filter the list
    searches = [item for item in searches if not item.startswith('---')]

    # if no searches then return empty list otherwise keep parsing
    if len(searches) == 0:
        return []
    else:
        # parse the searches and make a list of searches
        searches = [item for item in searches if not item.startswith('---')]
        searches = [re.sub(r'(\\.n|\\.n\s+:|\\)', ' ', item) for item in searches]
        searches = [re.sub(r'\s+:', ',', item) for item in searches]
        searches = [item.split(',') for item in searches]

        # Determine validity:
        # a valid search contains enabled==true and has clicks >= 3
        # store only valid searches in the list to return.
        valid_searches = []
        for item in searches:
            search_dict = {}
            for key in item:
                if key.split(':')[0] in ('search_id', 'enabled', 'clicks',
                                    'type', 'listings_sent', 'recommended'):
                    d_key = key.split(':')[0]
                    d_value = key.split(':')[1].strip()
                    search_dict[d_key] = d_value
            if search_dict['enabled'] == 'true' and int(search_dict.get('clicks', 0)) >= 3:
                valid_searches.append(search_dict)

        return valid_searches

def avg_listings_sent(valid_searches):
    num_listings = 0
    listings = 0
    for item in valid_searches:
        if item.get('listings_sent'):
            num_listings = num_listings + 1
            listings = listings + int(item.get('listings_sent'))

    if num_listings > 0:
        return np.round(np.sum(listings)/num_listings, 2)
    else:
        return 0

def type_of_search(valid_searches):
    '''
        Categorize the type of search given a list of searches.

        :params valid_searches: list of searches
        :return enum('rental_and_sale', 'sale', 'rental', 'none')
    '''
    rental = 0
    sale = 0
    for item in valid_searches:
        if item.get('type') == 'Rental':
            rental = rental + 1
        elif item.get('type') == 'Sale':
            sale = sale + 1
        else:
            pass

    if rental > 0 and sale > 0:
        return "rental_and_sale"
    elif rental > 0:
        return "rental"
    elif sale > 0:
        return "sale"
    else:
        return "none"

def list_of_valid_searches(valid_searches):
    '''
        Convert a list of lists to a single list

        :param valid_searches: list of lists
        :return search_list: single list of searches.
    '''
    search_list = []
    for item in valid_searches:
        if item.get('search_id'):
            search_list.append(item.get('search_id'))
    return search_list
