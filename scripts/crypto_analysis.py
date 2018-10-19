
# coding: utf-8
#Omar Isaias Flores Galicia
#18.01.2018


#Main Libs
import os
import sys
import traceback
import pandas as pd
import json
import requests
from requests import exceptions
from datetime import datetime, timedelta


#CONSTANT VARIABLES
API_URL_BASE   = 'https://www.alphavantage.co/query'
FUNCTION_TOKEN = 'DIGITAL_CURRENCY_DAILY'
SYMBOL_TOKEN   = 'BTC'
MARKET_TOKEN   = 'USD'
ASC_ORDER      = True
DESC_ORDER     = False
ERROR_TAG_JSON_PROP = 'Error Message'
PATH_TO_STORE = 'temp_records'
headers = {'Content-Type': 'application/json'}
FILE_DATA_COLUMNS = ["timestamp",
                "1a. open (USD)",
                    "1b. open (USD)",
                    "2a. high (USD)",
                    "2b. high (USD)",
                    "3a. low (USD)",
                    "3b. low (USD)",
                    "4a. close (USD)",
                    "4b. close (USD)",
                    "5. volume",
                    "6. market cap (USD)"]

#Function that request information from the API in JSON format
def get_data_from_api(api_token):
    try:
        payload = {'function':FUNCTION_TOKEN, 'symbol':SYMBOL_TOKEN, 'market':MARKET_TOKEN,'apikey':api_token}
        response = requests.get(API_URL_BASE,params=payload)

        if response.status_code >= 500:
            print('[!] [{0}] Server Error'.format(response.status_code))
            return None
        elif response.status_code == 404:
            print('[!] [{0}] URL not found: [{1}]'.format(response.status_code,api_url))
            return None
        elif response.status_code == 401:
            print('[!] [{0}] Authentication Failed'.format(response.status_code))
            return None
        elif response.status_code == 400:
            print('[!] [{0}] Bad Request'.format(response.status_code))
            return None
        elif response.status_code >= 300:
            print('[!] [{0}] Unexpected Redirect'.format(response.status_code))
            return None
        elif response.status_code == 200:
            print('URL API {0}'.format(response.url))
            json_data = json.loads(response.content.decode('utf-8'))
            return json_data
        else:
            print('[?] Unexpected Error: [HTTP {0}]: Content: {1}'.format(response.status_code, response.content))
    except exceptions.ConnectionError as e:
        print('[?] Connection Error: Check out internet connection or port access {0}'.format(e))
        response = 'No response'
        return None
    except ValueError:
        print('[?] Unexpected Error: [HTTP {0}]: Content: {1}'.format(response.status_code, response.content))
        return None
    return None

#Function that converts the JSON object on Pandas DataFrame
#Params: json_data = JSON object with data,
#       sort_asc_desc = True for ascending or False for descending sorting

def get_dataframe_from_json(json_data,sort_asc_desc):
    TIME_SERIES_API_TAG = 'Time Series (Digital Currency Daily)'
    JSON_OBJECT_COLUMNS = ["1a. open (USD)",
                    "1b. open (USD)",
                    "2a. high (USD)",
                    "2b. high (USD)",
                    "3a. low (USD)",
                    "3b. low (USD)",
                    "4a. close (USD)",
                    "4b. close (USD)",
                    "5. volume",
                    "6. market cap (USD)"]

    time_series_json_data = json_data[TIME_SERIES_API_TAG]
    daily_crypto_records_list = [];

    for timestamp_cryptocurrency in time_series_json_data:
        selected_row = []
        selected_row.append(pd.to_datetime(timestamp_cryptocurrency))
        for item in JSON_OBJECT_COLUMNS:
            selected_row.append(float(time_series_json_data[timestamp_cryptocurrency][item]))
        daily_crypto_records_list.append(selected_row)

    daily_crypto_dataframe = pd.DataFrame(data=daily_crypto_records_list,columns=FILE_DATA_COLUMNS).sort_values([FILE_DATA_COLUMNS[0]],ascending=sort_asc_desc)

    return daily_crypto_dataframe

#Save a dataframe to CSV file
#Params: filename = name of the file, path = place to stare the file,
#       df_crypto_prices = pandas dataframe, index_flag = True to store indexs or False to not save index
def save_dataframe_to_csv(filename,path,df_crypto_prices,index_flag):
    try:
        timestamp_for_file = int((datetime.now() - datetime.utcfromtimestamp(0)).total_seconds())
        file_name_crypto_csv = '{0}/{1}_{2}.csv'.format(path,filename,timestamp_for_file)
        df_crypto_prices.to_csv(file_name_crypto_csv,encoding='utf-8',index=index_flag)
    except Exception as e:
        print("[?] Error: creating the file {0} {1}. More information {2}".format(filename,path,e) )
        return False
    return file_name_crypto_csv

#Function which is used in lambda function to compute relative_span per each week data
def relative_span_calc(cols):
    return ((cols['close_weekly_max_price']-cols['close_weekly_min_price'])/cols['close_weekly_min_price'])

#Add timestamp from dataset as an index and drop unused columns
#Params: df_daily_crypto = pandas dataframe with dayly close price values.
def get_dataFrame_transformed(df_daily_crypto):
    df_daily_crypto['date_time_index'] = df_daily_crypto["timestamp"].apply( lambda df_daily_crypto : datetime(year=df_daily_crypto.year, month=df_daily_crypto.month, day=df_daily_crypto.day))
    df_daily_crypto.set_index(df_daily_crypto["date_time_index"],inplace=True)
    return df_daily_crypto

#Function which get weekly average and returns a dataframe with:
# DatetimeIndex and the close weekly caluclations taking
# as a base '4a. close (USD)' (value from the data API) values
#Params: df_daily_crypto = pandas dataframe
def get_weekly_average_dataFrame(df_daily_crypto):
    df_daily_crypto = df_daily_crypto['4a. close (USD)'].astype('float')
    df_weekly_close_value = pd.Series.to_frame(df_daily_crypto.resample('W').mean())
    df_weekly_close_value.columns = ['close_weekly_average_price']
    return df_weekly_close_value

#Computes the relative_span for all the data contained in the DataFrame And
#Prints the week with the highest relative_span
def compute_relative_span_from_dataframe(df_daily_crypto):
    df_weekly_close_minmax = pd.Series.to_frame(df_daily_crypto.resample('W').max())
    df_weekly_close_minmax.columns = ['close_weekly_max_price']
    df_weekly_close_value_min = pd.Series.to_frame(df_daily_crypto.resample('W').min())
    df_weekly_close_value_min.columns = ['close_weekly_min_price']
    df_weekly_close_minmax['close_weekly_min_price'] = df_weekly_close_value_min['close_weekly_min_price']
    df_weekly_close_minmax.index = pd.to_datetime(df_weekly_close_minmax.index, unit='s')
    df_weekly_close_minmax['relative_span'] = df_weekly_close_minmax[['close_weekly_max_price','close_weekly_min_price']].apply(relative_span_calc,axis=1)
    week_datetime = df_weekly_close_minmax['relative_span'].idxmax()
    week_datetime = '{0}-{1}-{2}'.format(week_datetime.year,week_datetime.month,week_datetime.day)
    print('The week that finalized in \'{0}\' has the greates relative_span = {1}.'.format(week_datetime,df_weekly_close_minmax['relative_span'].max()))
    return None

#Main function to call
#Calculates Weekly average and save on disk.
#And Calculates relative span for each week and prints the highest week with relative span in the dataset.
def compute_statistics_from_dataset():

    try:
        api_key  = os.environ['API_KEY']
        dir_name = os.environ['FOLDER_NAME']

        json_data = get_data_from_api(api_key)


        if (len(json_data) > 1) and (json_data.get(ERROR_TAG_JSON_PROP) is None) :
            crypto_daily_dataframe = get_dataframe_from_json(json_data,ASC_ORDER)
            #Save file with the daily records in a CSV file
            filename = '{0}_{1}_{2}'.format(FUNCTION_TOKEN,SYMBOL_TOKEN,MARKET_TOKEN)
            is_created = save_dataframe_to_csv(filename,dir_name,crypto_daily_dataframe,False)

            if is_created is not False:
                print('{0} file has been created'.format(is_created))

            crypto_daily_dataframe = get_dataFrame_transformed(crypto_daily_dataframe)
            df_weekly_close_value = get_weekly_average_dataFrame(crypto_daily_dataframe)
            filename = 'WEEKLY_AVERAGE_PRICE_{0}_{1}'.format(SYMBOL_TOKEN,MARKET_TOKEN)
            is_created = save_dataframe_to_csv(filename,dir_name,df_weekly_close_value,True)

            if is_created is not False:
                print('{0} file has been created'.format(is_created))

            crypto_daily_dataframe = crypto_daily_dataframe['4a. close (USD)'].astype('float')
            compute_relative_span_from_dataframe(crypto_daily_dataframe)
        else:
            print('[?] get_data_from_api - Unexpected Error querying the API, error message: {0}'.format(json_data[ERROR_TAG_JSON_PROP]))
    except TypeError as e:
        tracing_error_module = traceback.format_exc()
        print('[?] Unexpected Error: \n {0}'.format(tracing_error_module))
    except KeyError:
        print('[?] Enviroment Error: Be sure you are adding correctly the API_KEY and FILE_NAME variables.')

print("Computing calculations in the crypto dataset...")
compute_statistics_from_dataset()
