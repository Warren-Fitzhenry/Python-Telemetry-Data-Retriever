import json 
import requests
# add your username and password to the account_details file

try: 
    import account_details
except:
    raise NameError("Please open 'blank_account_details.py', fill in your " 
        "username and password and save the file as 'account_details.py' "
        "then re-run the script.")

import numpy as np
import pandas as pd 
import pytz
import datetime as dt
import dateutil.parser as dparser
import matplotlib.pyplot as plt

global token_expiry
token_expiry = "2000-01-01T00:00:00"


def get_imeis():
    products = api_get(endpoint='products', as_df=True)
    products.to_csv('./products.csv')


def simple_example():
    imei = '013777004512610'
    # filtering for a single imei
    filters = [dict(name='product_imei', op='eq', val=imei)]
    # choosing as_df = False so that product can be used directly in the next call
    product = api_get(endpoint='products', filters=filters, as_df =False)
    # product[0] is the first (and only) dictionary in the list
    start = "2015-05-01 00:12:00" 
    end = "2015-05-01 00:32:00"
    data = get_telemetry2(product[0], start, end)
    # saves the data to a csv file. 
    data.to_csv('./telemetry_{}_{}_{}.csv'.format(imei, start[:10], end[:10]))
    # plotting the data with each metric on a subplot
    data.plot(subplots=True)
    plt.show()


def complex_example():

    # the endpoint used to retrieve the dashboard at smart.bboxx.co.uk
    units = api_get(endpoint='dashboard2', as_df=True)
    print units.shape

    # extract entity names from entity column and select
    units['entity_id'] = units.entities.apply(lambda x: x[0]['entity_id'])
    # selecting only the units that belong to BBOXX Capital
    bxc_units = units[units.entity.str.contains('BBOXX Capital')]
    print bxc_units.shape
    # only units that have been activated will have sent data
    activated = bxc_units[bxc_units.state == 'activated']
    print activated.shape
    # only units that have connected recently
    activated_and_connected = activated[activated.recent_connection > "2016-10-01"]
    print activated_and_connected.shape
    # sorting dataframe so units that connected most recently are first
    activated_and_connected.sort_values('recent_connection', ascending=False, inplace=True)

    for unit in activated_and_connected.head().to_records(): 
        data = get_telemetry(unit, "2016-10-01", "2016-10-02", index_type='datetime')    
        if data is not None:
            print data.voltage.max() 


def token_validity_checker():
    """
    Description
    ===========
    Used by smartapi_login() to check whether you need to log in again. It's 
    useful if you run a script that makes many calls over a few hours. 

    """
    token_valid = False
    token = dparser.parse(token_expiry).replace(tzinfo=None)
    now = dt.datetime.utcnow()

    time_remaining = token - now
    if time_remaining > dt.timedelta(minutes=1):
        token_valid = True
    
    return token_valid


def smartapi_login():
    """
    Description
    ===========
    Logs into the api.

    """
    if token_validity_checker() is False:
        url = 'http://smartapi.bboxx.co.uk/v1/auth/login'
        # Enter your username and password in account_details
        fields = account_details.main()
        for i in fields.keys():
            if fields[i] == 'blank':
                raise StandardError("Check that you have the correct" 
                    " username and password in account_details.py")

        pre_login = dt.datetime.utcnow()
        r = requests.post(url, data=fields)
        if r.status_code != 200:
            print "HTTP Status code", r.status_code
            print r.text
            raise StandardError("Login failed")

        post_login = dt.datetime.utcnow()

        global token_expiry
        token_expiry = r.json()["message"]["login_successful"]["token_expiry"]

        token = r.json()["message"]["login_successful"]["API_token"]
        print "Logged in to API"
        global headers
        headers = {
                    'Content-Type':'application/json',
                    'Authorization':'Token token='+ token
                    }

        return headers

    else:
        return headers


def api_get(endpoint='', filters=[], limit=0, as_df=False):
    """
    Description
    ===========
    This function can be used for most GET requests from the API, with 
    the exception of telemetry.  
    
    Parameters
    ==========
    endpoint: <string>
        Should be a valid endpoint to appent to http://smartapi.bboxx.co.uk/v1/
        API documentation: http://smartapi.bboxx.co.uk/v1/docs/api_docs.html 

    filters: list of dictionaries i.e. [{}] or [{},{},...] 
        Documentation on how to make search queries: 
        http://flask-restless.readthedocs.org/en/latest/searchformat.html
    
    limit: integer 
        Defines the maximum number of results to return from the request. 
        Default = 0, which returns all results. 

    as_df: boolean
        If as_df = True, the results will be returned as a pandas.DataFrame. 
        If False, the results will be returned as a list, which can is useful
        for looping through multiple units. 

    Example
    =======
    # to get product details for unit with IMEI 013777004512610

    filters = [dict(name='product_imei', op='eq', val='013777004512610')]
    unit = api_get(endpoint='products', filters=filters, as_df=False)
    """

    headers = smartapi_login()
    url_trunk = 'http://smartapi.bboxx.co.uk/v1/'
    url = url_trunk + endpoint
    data = json.dumps({})
    params = dict(q=json.dumps(dict(filters=filters)))
    if limit > 0: 
        params = dict(q=json.dumps(dict(filters=filters, limit=limit)))

    if endpoint == 'dashboard2':
        r = requests.get(url=url, headers=headers)
    else:
        r = requests.get(url=url, data=data, params=params, headers=headers)

    if r.status_code != 200:
        print r.status_code
        print r.text
        return 
    result = json.loads(r.content)

    if endpoint == 'current_states': 
        df = pd.DataFrame(result)
        df = df.transpose().reset_index()
        data = df.rename(columns={'index': 'product_imei'})

    elif endpoint == 'dashboard': 
        df = pd.DataFrame(result)
        df = df.transpose().reset_index()
        df = df.drop("index",1)
        data = df

    elif endpoint == 'dashboard2':
        data = pd.DataFrame(r.json()['data'])
        data['entity'] = data.entities.apply(lambda x: x[0]['name'])
        if as_df == False:
            return data.to_records() 

    else: 
        data = result['objects']
        if as_df is True:
            data = pd.DataFrame(data)

    return data


def get_telemetry(product, start, end, index_type='datetime', 
                    localize_index=False):
    """
    Description
    ===========
    Returns telemetry data for one product between a start time and end time
    as a pandas.DataFrame. 
    
    Parameters
    ==========
    product: <dict> or <numpy.core.records.record>
        The dictionary must include the product's IMEI as 'product_imei'. If 
        localize_index is True then it must also contain the 'entity_id'.

    start: string
        The start of the data slice. Must be in the form "yyyy-MM-dd" or 
        "yyyy-MM-dd hh:mm:ss".
    
    end: string
        The end of the data slice. Must be in the form "yyyy-MM-dd" or 
        "yyyy-MM-dd hh:mm:ss".
    
    index_type: ['datetime', 'unix']
        'datetime' sets the index of the dataframe to 'datetime' type (easy to
        read). 'unix' returns a unix timestamp. 
    
    localize_index: boolean
        You can choose whether to return the data with timestamps in UTC (set 
        this parameter to False) or the local time of the unit (set this to 
        True).  

    """
    if type(product) != dict:
        if not isinstance(product, np.core.records.record):
            print type(product)
            raise TypeError("'product' must be dict " 
                                "or numpy.core.records.record")
    elif 'product_imei' not in product.keys():
        raise NameError("Product dictionary does not include 'product_imei'")
    elif localize_index == True and 'entity_id' not in product.keys():
        raise NameError("Product dictionary does not include 'entity_id' "
                            "unable to localize the time index.")

    imei = product['product_imei']

    valid_index_types = ['datetime', 'unix']
    if index_type not in valid_index_types:
        print "Please choose a valid index type from:"
        for i in valid_index_types:
            print i
        raise TypeError("'index_type' is not valid")

    headers = smartapi_login()
    url = 'http://smartapi.bboxx.co.uk/v1/products/'+imei+'/rm_data'
    data = json.dumps(dict(start_time=start,
                  end_time=end))

    r = requests.get(url=url, data=data, headers=headers)

    if r.status_code != 200:
        print r.status_code
        print r.text
        raise StandardError("Failed to retrieve data.")

    all_data = json.loads(r.content)

    names = ['time', 'sequence_number', 'value']

    if len(all_data['current']) == 0:
        "No data for", imei
        return
    else:
        print "Data retrieved"

    current = pd.DataFrame(all_data['current'][0]['points'], columns=names)
    voltage = pd.DataFrame(all_data['voltage'][0]['points'], columns=names)
    temperature = pd.DataFrame(all_data['temperature'][0]['points'], columns=names)
    temperature = temperature.drop('sequence_number', axis=1)

    data = current
    data['voltage'] = voltage['value']
    data = data.merge(temperature, on='time', how='outer')
    data = data.rename(columns={'value_x': 'current',
                                'value_y': 'temperature'})
    data = data.set_index('time', drop=True)
    data = data.drop('sequence_number', axis=1)

    if index_type == 'datetime':
        data.index = pd.to_datetime(data.index, unit='ms')

    data.sort_index(inplace=True)

    if localize_index == True: 
        data = localize(data, product)
    return data


def lookup_timezone(entity_id):
    tz_lookup = {  '4':'Africa/Nairobi',
                    '5':'Africa/Kigali',
                    '6':'Africa/Kampala'}
    return tz_lookup[str(entity_id)]


def localize_datetime(product, date):

    date = dt.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    localtz = pytz.timezone(lookup_timezone(product['entity_id']))
    dt_aware = localtz.localize(date)

    new_date = dt_aware.astimezone(pytz.utc).isoformat()

    return  new_date[:10]+' '+new_date[11:19]


def get_telemetry2(product, start, end, index_type='datetime', 
                    localize_index=False, limit=None, where=None,
                    metrics=['voltage', 'current', 'temperature'],
                    ds_interval=None, ds_function=None):
    """
    Description
    ===========
    Returns telemetry data for one product between a start time and end time
    as a pandas.DataFrame. It is the same as get_telemetry() apart from the
    new argument "metrics" which enables current_out and current_in data to
    be queried for BBOXX Home units. Latest docs on the endpoint can be
    found here: http://docs.smart.bboxx.co.uk/#reading-data-for-a-product
    
    Parameters
    ==========
    product: <dict> or <numpy.core.records.record>
        The dictionary must include the product's IMEI as 'imei'. If 
        localize_index is True then it must also contain the 'entity_id'.

    start: string
        The start of the data slice. Must be in the form "yyyy-MM-dd hh:mm:ss".
    
    end: string
        The end of the data slice. Must be in the form "yyyy-MM-dd" or 
        "yyyy-MM-dd hh:mm:ss".
    
    index_type: ['datetime', 'unix']
        'datetime' sets the index of the dataframe to 'datetime' type (easy to
        read). 'unix' returns a unix timestamp. 
    
    localize_index: boolean
        You can choose whether to return the data with timestamps in UTC (set 
        this parameter to False) or the local time of the unit (set this to 
        True).

    limit: integer
        Limit the number of data points returned by the request. 

    where: string
        An optional where clause using InfluxQL e.g. "'voltage' > 14.0".
        More info: https://docs.influxdata.com/influxdb/v1.0/query_language/data_exploration/#the-where-clause

    metrics: list
        a list of metrics. All units have current, voltage and temperature.
        BBOXX Homes have current_out and current_in in addition to these. 

    ds_interval: string
        A time interval to downsample to. The following are valid units: 
            u microseconds
            ms milliseconds
            s seconds
            m minutes
            h hours
            d days
            w weeks
        e.g. one point every day would be "1d", one point every 10 minutes would be "10m"

    ds_function: string
        A function to downsample, e.g. mean(), median(), min(), max()

    """
    if type(product) != dict:
        if not isinstance(product, np.core.records.record):
            print type(product)
            raise TypeError("'product' must be dict " 
                                "or numpy.core.records.record")
    elif 'product_imei' not in product.keys():
        raise NameError("Product dictionary does not include 'product_imei'")
    elif localize_index == True and 'entity_id' not in product.keys():
        raise NameError("Product dictionary does not include 'entity_id' "
                            "unable to localize the time index.")

    if localize_index == True:
        start = localize_datetime(product, start)
        end = localize_datetime(product, end)
       
    imei = product['product_imei']

    valid_index_types = ['datetime', 'unix']
    if index_type not in valid_index_types:
        print "Please choose a valid index type from:"
        for i in valid_index_types:
            print i
        raise TypeError("'index_type' is not valid")

    headers = smartapi_login()
    url = 'http://smartapi.bboxx.co.uk/v1/products/{}/data'.format(imei)
    
    params = {
        'start': start,
        'end': end,
        'fields': metrics,
        'limit': limit, 
        'where': where,
        'ds_interval': ds_interval,
        'ds_function': ds_function
        }

    r = requests.get(url=url, params=params, headers=headers)


    if r.status_code != 200:
        print imei
        print r.status_code
        print r.text
        raise StandardError("Failed to retrieve data.")  

    all_data = r.json()["data"]

    DF = []
    for metric in metrics:
        names = [metric, 'time']
        try:
            df = pd.DataFrame(all_data[metric], columns=names)
        except:
            "No data for", imei
            return
        df = df.set_index('time', drop=True)
        DF.append(df)

    if len(DF) > 1:
        data = DF[0].join(DF[1:])
    else:
        data = DF[0]
    data.sort_index(inplace=True)

    if index_type == 'datetime':
        data.index = pd.to_datetime(data.index)

    # data.sort_index(inplace=True)

    if localize_index == True: 
        data = localize(data, product)
    return data


def localize(df, product):
    """
    Description
    ===========
    Takes a pandas.DataFrame of telemetry data and dictionary-like data about
    the unit to find its timezone and return a dataframe of data in the 
    local timezone of the unit. Only works for Kenya, Rwanda and Uganda. 

    Parameters
    ==========
    df: <pandas.core.DataFrame>
        A DataFrame of telemetry data.

    product:  <dict> or <numpy.core.records.record>
        The dictionary must include the product's IMEI as 'product_imei' and entity_id
        as 'entity_id'. 
    """
    try:
        tz = lookup_timezone(str(product['entity_id']))
        print tz
    except:
        print "WARNING: unable to find the timezone for", product['product_imei']
        print "Timezone will remain UTC."
        return df

    tz = pytz.timezone(tz)
    df.index = df.index.tz_localize(pytz.utc).tz_convert(tz)

    return df


if __name__ == '__main__':
    # complex_example()
    simple_example()
