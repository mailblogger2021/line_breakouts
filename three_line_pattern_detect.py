import pandas as pd
import os
from multiprocessing import Pool
import threading
import datetime
from pytz import timezone
import requests
from scipy.stats import linregress
import itertools
import logging
import json
import traceback
import time
import traceback
import sys

import get_candle_data as get_candle_data
import functions
import telegram_message_send 

print("Start...")
window=10
percentage = 1
pivot_line_count  = 3 #3
two_line_count = 2
time_frame = "day"
three_line_file_name = f"three_line_alerts_{time_frame}.xlsx"
two_line_file_name = f"two_line_alerts_{time_frame}.xlsx"
data_store_file_name = f"data_store_{time_frame}.json"
start_time = time.time()
max_execution_time = 5*3600
# max_execution_time = 300

# isExist = os.path.exists(three_line_file_name)
# three_line_alert_df = pd.DataFrame()
# if(isExist):
#     three_line_alert_df = pd.read_excel(three_line_file_name)

# isExist = os.path.exists(two_line_file_name)
# two_line_alert_df = pd.DataFrame()
# if(isExist):
#     two_line_alert_df = pd.read_excel(two_line_file_name)

# if os.path.exists(data_store_file_name):
#     with open(data_store_file_name, "r") as file:
#         data_store = json.load(file)
# else:
#     data_store = {}

# logging.basicConfig(filename=f'logfile_{time_frame}.log',level=logging.INFO, format='%(asctime)s -%(levelname)s - %(message)s')
# logging.info(f"Started...")

def process_row(candles,stock_name,function_name,number_of_calls=0):
    # print(function_name.__name__)
    # return
    global three_line_alert_df,two_line_alert_df,data_store
    logging.info(f'{stock_name} - {function_name.__name__} function started')
    if(number_of_calls==0):
        candles[f'{function_name.__name__}'] = candles.apply(lambda row: function_name(candles,stock_name,
                                                            row.name, backcandles=15, window=window,
                                                            ), axis=1)
    else:
        candles['backup'] = candles[f'{function_name.__name__}']
        candles[f'{function_name.__name__}'] = candles.shift(-number_of_calls).iloc[-number_of_calls:].apply(
                                                    lambda row: function_name(candles,stock_name,
                                                    row.name, backcandles=15, window=window,
                                                    ), axis=1)
        candles[f'{function_name.__name__}'] = candles[f'{function_name.__name__}'].fillna(candles['backup'])
    logging.info(f'{stock_name} - last n {function_name.__name__} function Ended')
    three_line_alert_df.to_excel(three_line_file_name,index=False)
    two_line_alert_df.to_excel(two_line_file_name,index=False)
    logging.info(f'{stock_name} - {"last n" if number_of_calls !=0 else ""} preparing_for_candles function Ended')
    logging.info(f'{stock_name} - alerts file Saved')

def generate_url(rows, time_frame, is_history_starting_from=False, is_add_indicator=True,number_of_time_called=0):
    end_time = time.time()
    elapsed_time = end_time - start_time
    stock_name = rows['STOCK NAME']
    if elapsed_time > max_execution_time:
        logging.info(f"Max time reached....")
        return
    if(number_of_time_called > 10):
        logging.info(f"{stock_name} - {number_of_time_called} - Max time tried....")
        return
    os.makedirs(f"stock_historical_data/{time_frame}", exist_ok=True)
    global three_line_alert_df,two_line_alert_df,data_store
    try:
        if not (time_frame in data_store and stock_name in data_store[time_frame]):
            number_of_time_called += 1
            file_name = f"stock_historical_data/{time_frame}/{stock_name}.xlsx"
            stock_data_historical = pd.DataFrame()  # Initialize with an empty DataFrame
            isExist = os.path.exists(file_name)
            if isExist:
                logging.info(f"{stock_name} - Reading existing data...")
                stock_data_historical = pd.read_excel(file_name)
                is_history_starting_from = False
            with requests.Session() as session:
                logging.info(f"{stock_name} - kite url call started...")
                logging.info(f"{stock_name} - is_history_starting_from - {is_history_starting_from}")
                candles = get_candle_data.get_kite_url(session, rows, time_frame, is_history_starting_from, is_add_indicator,number_of_time_called)
                logging.info(f"{stock_name} - kite url call Ended...")

            candles = pd.concat([stock_data_historical, candles], axis=0)
            candles["Datetime"] = pd.to_datetime(candles["Datetime"], format='%d-%m-%Y %H:%M:%S')
            candles = candles.drop_duplicates(subset=['Datetime'], keep='first')\
                        .sort_values(by='Datetime')\
                        .reset_index(drop=True)
            # is_history_starting_from = True
            logging.info(f'{stock_name} - {"last n" if is_history_starting_from !=True else ""} preparing_for_candles function started')
            logging.info(f'{stock_name} - {"last n" if is_history_starting_from !=True else ""} isPivot function started')
            if(is_history_starting_from):

                candles['isPivot'] = candles.apply(lambda row: isPivot(candles,stock_name,row.name,window), axis=1)
                logging.info(f'{stock_name} - isPivot function Ended')
                threads = []
                for function_name in [detect_structure, two_line_structure]:
                    thread = threading.Thread(target=process_row, args=(candles, stock_name, function_name))
                    threads.append(thread)
                    thread.start()

                for thread in threads:
                    thread.join()
            else:
                number_of_calls = get_candle_data.maximum_candle_limit[time_frame]
                candles['backup'] = candles['isPivot']
                candles['isPivot'] = candles.shift(-number_of_calls).iloc[-number_of_calls:].apply(lambda row: isPivot(candles, stock_name, row.name, window), axis=1)
                candles['isPivot'] = candles['isPivot'].fillna(candles['backup'])
                logging.info(f'{stock_name} - {"last n" if is_history_starting_from !=True else ""} isPivot function Ended')
                
                threads = []
                for function_name in [detect_structure, two_line_structure]:
                    thread = threading.Thread(target=process_row, args=(candles, stock_name, function_name, number_of_calls))
                    threads.append(thread)
                    thread.start()

                for thread in threads:
                    thread.join()

            candles.to_excel(file_name, index=False)
            session.close()
        else:
            logging.info(f"{stock_name} - this stock already completed")
        if time_frame in data_store:
            if stock_name not in data_store[time_frame]:
                data_store[time_frame].append(stock_name)
        else:
            data_store[time_frame] = [stock_name]
    except requests.RequestException as e:
        logging.info(f"{stock_name} - An error occurred: {e}")
        traceback_msg = traceback.format_exc()
        logging.info(f"{stock_name} - Error : {traceback_msg}")
        # if isinstance(e, requests.ConnectionError) and hasattr(e, 'response') and e.response.status_code == 429:
        if isinstance(e, requests.ConnectionError) and hasattr(e, 'response') and e.response and e.response.status_code == 429:
            retry_after = e.response.headers.get('Retry-After')
            if retry_after:
                retry_after = int(retry_after)
                logging.info(f"{stock_name} - Too many requests. Retrying after {retry_after} seconds...")
                time.sleep(retry_after)
                generate_url(rows, time_frame, is_history_starting_from, is_add_indicator,number_of_time_called+1)
            else:
                logging.info(f"{stock_name} - Retry-After header not found. Retrying after 5 seconds...")
                time.sleep(5)
                generate_url(rows, time_frame, is_history_starting_from, is_add_indicator,number_of_time_called+1)
        else:
            logging.info(f"{stock_name} - Retrying after 5 seconds...")
            time.sleep(5)
            generate_url(rows, time_frame, is_history_starting_from, is_add_indicator,number_of_time_called+1)
    except Exception as e:
        logging.info(f"{stock_name} - Error in generate_url function: {e}")
        traceback_msg = traceback.format_exc()
        logging.info(f"{stock_name} - Error : {traceback_msg}")
    if(number_of_time_called):
        logging.info(f"{stock_name} - generate_url function ended...")
    with open(data_store_file_name, "w") as file:
        json.dump(data_store, file,indent = 4)


def isPivot_old(df,stock_name,candle, window):
    """
    function that detects if a candle is a pivot/fractal point
    args: candle index, window before and after candle to test if pivot
    returns: 1 if pivot High, 2 if pivot Low, 3 if both and 0 default
    """
    try:
        if candle-window < 0 or candle+window >= len(df):
            return 0
        
        pivotHigh = 1
        pivotLow = 2
        for i in range(candle-window, candle+window+1):
            if df.iloc[candle].Low > df.iloc[i].Low:
                pivotLow=0
            if df.iloc[candle].High < df.iloc[i].High:
                pivotHigh=0
        if (pivotHigh and pivotLow):
            return 3
        elif pivotHigh:
            return pivotHigh
        elif pivotLow:
            return pivotLow
        else:
            return 0
        
    except Exception as e:
        logging.info(f"{stock_name} - Error in isPivot function: {e}")
        traceback_msg = traceback.format_exc()
        logging.info(f"{stock_name} - Error : {traceback_msg}")

def isPivot(df,stock_name,candle, window):
    """
    function that detects if a candle is a pivot/fractal point
    args: candle index, window before and after candle to test if pivot
    returns: 1 if pivot High, 2 if pivot Low, 3 if both and 0 default
    """
    try:
        if candle - window < 0 or candle + window >= len(df):
            return 0
        
        candle_data = df.iloc[candle]
        window_data = df.iloc[candle - window:candle + window + 1]

        pivotLow = (window_data['Low'] >= candle_data['Low']).all()
        pivotHigh = (window_data['High'] <= candle_data['High']).all()

        if pivotHigh and pivotLow:
            return 3
        elif pivotHigh:
            return 1
        elif pivotLow:
            return 2
        else:
            return 0
    except Exception as e:
        logging.info(f"{stock_name} - Error in isPivot function: {e}")
        traceback_msg = traceback.format_exc()
        logging.info(f"{stock_name} - Error : {traceback_msg}")
    
def plus_minus_01_percent(combination,percentage):
    try:
        x_values,y_values = zip(*combination)
        slope, intercept, _, _, _ = linregress(x_values[0:2], y_values[0:2])
        predicted_y_value = slope * x_values[-1] + intercept
        actual_y_value = y_values[-1]

        percent_difference = abs(predicted_y_value - actual_y_value) / actual_y_value * 100
        # print(percent_difference,percentage)
        return slope, intercept,percent_difference <= percentage

    except Exception as e:
        logging.info(f"{combination} - Error in Plus minus 1 percents function: {e}")
        traceback_msg = traceback.format_exc()
        logging.info(f"{combination} - message : {traceback_msg}")

def detect_structure(df,stockname,candle, backcandles, window):
    if (candle <= (backcandles+window)) and (candle+window+1 >= len(df)):
        return 0
    # if(candle < 5818):
        # return 0
    # print(candle)
    localdf = df.iloc[0:candle-window] 
    Highs = localdf[localdf['isPivot'] == 1].High.tail(5).values
    x_values_Highs = localdf[localdf['isPivot'] == 1].index[-5:].values
    Highs_values = pd.DataFrame(localdf[localdf['isPivot'] == 1].High.tail(5))

    Lows = localdf[localdf['isPivot'] == 2].Low.tail(5).values
    x_values_Lows = localdf[localdf['isPivot'] == 2].index[-5:].values
    Lows_values = pd.DataFrame(localdf[localdf['isPivot'] == 2].Low.tail(5))
    levelbreak = 0
    global three_line_alert_df
    if Lows_values.shape[0]>=pivot_line_count and candle-window-1 == Lows_values.index[-1]:
        combinations = list(itertools.combinations(zip(x_values_Lows, Lows), pivot_line_count))
        leatest_combinations = [[(index_low[0], index_low[1]) for index_low in combination] for combination in combinations]
        for combination in leatest_combinations:
            slope, intercept , is_line= plus_minus_01_percent(combination,percentage)
            if(is_line):
                levelbreak = 1
                alert = pd.DataFrame(combination, columns=['index', 'value']).set_index('index').copy(deep=True)
                row_to_append = pd.DataFrame({
                    'stockname' : stockname,
                    'alert_date' : [df.iloc[candle].Datetime],
                    'rowNumber' : candle,                    
                    'date1': [df.iloc[alert.index[0]].Datetime],
                    'row1': [alert.index[0]],
                    'value1': [alert.iloc[0, 0]],
                    'date2': [df.iloc[alert.index[1]].Datetime],
                    'row2': [alert.index[1]],
                    'value2': [alert.iloc[1, 0]],
                    'date3': [df.iloc[alert.index[2]].Datetime] if pivot_line_count>2 else 0,
                    'row3': [alert.index[2]] if pivot_line_count>2 else 0,
                    'value3': [alert.iloc[2, 0]] if pivot_line_count>2 else 0,
                    'buyORsell' : 'Low' if levelbreak==1 else 'High',
                    "slope" : slope,
                    "intercept" : intercept,
                    "window_size":window,
                    "percentage_value" : percentage,
                    "pivot_line_count" : pivot_line_count
                })
                three_line_alert_df = pd.concat([three_line_alert_df, row_to_append], ignore_index=True)
    if Highs_values.shape[0]>=pivot_line_count and candle-window-1 == Highs_values.index[-1]:
        combinations = list(itertools.combinations(zip(x_values_Highs, Highs), pivot_line_count))
        leatest_combinations = [[(index_high[0], index_high[1]) for index_high in combination] for combination in combinations]
        for combination in leatest_combinations:
            slope, intercept , is_line= plus_minus_01_percent(combination,percentage)
            if(is_line):
                levelbreak = 2
                alert = pd.DataFrame(combination, columns=['index', 'value']).set_index('index').copy(deep=True)
                # print(alert)
                row_to_append = pd.DataFrame({
                    'stockname' : stockname,
                    'alert_date' : [df.iloc[candle].Datetime],
                    'rowNumber' : candle,
                    'date1': [df.iloc[alert.index[0]].Datetime],
                    'row1': [alert.index[0]],
                    'value1': [alert.iloc[0, 0]],
                    'date2': [df.iloc[alert.index[1]].Datetime],
                    'row2': [alert.index[1]],
                    'value2': [alert.iloc[1, 0]],
                    'date3': [df.iloc[alert.index[2]].Datetime] if pivot_line_count>2 else 0,
                    'row3': [alert.index[2]] if pivot_line_count>2 else 0,
                    'value3': [alert.iloc[2, 0]] if pivot_line_count>2 else 0,
                    'buyORsell' : 'Low' if levelbreak==1 else 'High',
                    "slope" : slope,
                    "intercept" : intercept,
                    "window_size":window,
                    "percentage_value" : percentage,
                    "pivot_line_count" : pivot_line_count
                })
                three_line_alert_df = pd.concat([three_line_alert_df, row_to_append], ignore_index=True)

    return levelbreak

def two_line_structure(df,stockname,candle, backcandles, window):
    if (candle <= (backcandles+window)) and (candle+window+1 >= len(df)):
        return 0
    localdf = df.iloc[0:candle-window] 
    Highs = localdf[localdf['isPivot'] == 1].High.tail(3).values
    x_values_Highs = localdf[localdf['isPivot'] == 1].index[-3:].values
    Highs_values = pd.DataFrame(localdf[localdf['isPivot'] == 1].High.tail(3))

    Lows = localdf[localdf['isPivot'] == 2].Low.tail(3).values
    x_values_Lows = localdf[localdf['isPivot'] == 2].index[-3:].values
    Lows_values = pd.DataFrame(localdf[localdf['isPivot'] == 2].Low.tail(3))
    levelbreak = 0
    global two_line_alert_df
    if Lows_values.shape[0]>=two_line_count and candle-window-1 == Lows_values.index[-1]:
        combinations = list(itertools.combinations(zip(x_values_Lows, Lows), two_line_count))
        leatest_combinations = [[(index_low[0], index_low[1]) for index_low in combination] for combination in combinations]
        for combination in leatest_combinations:
            x_values,y_values = zip(*combination)
            percent_difference = abs(y_values[0] - y_values[1]) / y_values[1] * 100
            is_line = percent_difference<=1
            if(is_line):
                levelbreak = 1
                alert = pd.DataFrame(combination, columns=['index', 'value']).set_index('index').copy(deep=True)
                row_to_append = pd.DataFrame({
                    'stockname' : stockname,
                    'alert_date' : [df.iloc[candle].Datetime],
                    'rowNumber' : candle,                    
                    'date1': [df.iloc[alert.index[0]].Datetime],
                    'row1': [alert.index[0]],
                    'value1': [alert.iloc[0, 0]],
                    'date2': [df.iloc[alert.index[1]].Datetime],
                    'row2': [alert.index[1]],
                    'value2': [alert.iloc[1, 0]],
                    'buyORsell' : 'Low',
                    "window_size":window,
                    "percentage_value" : percentage,
                    "two_line_count" : two_line_count
                })
                two_line_alert_df = pd.concat([two_line_alert_df, row_to_append], ignore_index=True)

    if Highs_values.shape[0]>=two_line_count and candle-window-1 == Highs_values.index[-1]:
        combinations = list(itertools.combinations(zip(x_values_Highs, Highs), two_line_count))
        leatest_combinations = [[(index_high[0], index_high[1]) for index_high in combination] for combination in combinations]
        for combination in leatest_combinations:
            x_values,y_values = zip(*combination)
            percent_difference = abs(y_values[0] - y_values[1]) / y_values[1] * 100
            is_line = percent_difference<=1
            if(is_line):
                levelbreak = 2
                alert = pd.DataFrame(combination, columns=['index', 'value']).set_index('index').copy(deep=True)
                row_to_append = pd.DataFrame({
                    'stockname' : stockname,
                    'alert_date' : [df.iloc[candle].Datetime],
                    'rowNumber' : candle,
                    'date1': [df.iloc[alert.index[0]].Datetime],
                    'row1': [alert.index[0]],
                    'value1': [alert.iloc[0, 0]],
                    'date2': [df.iloc[alert.index[1]].Datetime],
                    'row2': [alert.index[1]],
                    'value2': [alert.iloc[1, 0]],
                    'buyORsell' : 'High',
                    "window_size":window,
                    "percentage_value" : percentage,
                    "two_line_count" : two_line_count
                })
                two_line_alert_df = pd.concat([two_line_alert_df, row_to_append], ignore_index=True)
    return levelbreak

def save_files():
    global three_line_alert_df,two_line_alert_df,data_store
    try:
        three_line_alert_df.drop_duplicates(subset=['stockname','date1', 'row1', 'value1', 'date2', 'row2', 'value2', 'date3', 'row3', 'value3', 'buyORsell', 'slope', 'intercept','window_size','percentage_value','pivot_line_count'], keep='first', inplace=True)
        two_line_alert_df.drop_duplicates(subset=['stockname','date1', 'row1', 'value1', 'date2', 'row2', 'value2', 'buyORsell','window_size','percentage_value','two_line_count'], keep='first', inplace=True)
        three_line_alert_df.to_excel(three_line_file_name,index=False)
        two_line_alert_df.to_excel(two_line_file_name,index=False)
        logging.info(f'All stocks - alerts file Saved')
    except Exception as e:
        logging.info(f"Error in saving alerts file: {e}")
        traceback_msg = traceback.format_exc()
        logging.info(f"Error : {traceback_msg}")

    try:
        with open(data_store_file_name, "w") as file:
            json.dump(data_store, file,indent = 4)
        logging.info(f'All stocks - json file saved')
    except Exception as e:
        logging.info(f"Error in saving json file: {e}")
        traceback_msg = traceback.format_exc()
        logging.info(f"Error : {traceback_msg}")

if __name__=="__main__":
    try:
        if len(sys.argv) >= 2:
            time_frame = sys.argv[1]
        
        logging.basicConfig(filename=f'logfile_{time_frame}.log',level=logging.INFO, format='%(asctime)s -%(levelname)s - %(message)s')
        logging.info(f"Started...")

        three_line_file_name = f"three_line_alerts_{time_frame}.xlsx"
        two_line_file_name = f"two_line_alerts_{time_frame}.xlsx"
        data_store_file_name = f"data_store_{time_frame}.json"

        isExist = os.path.exists(three_line_file_name)
        three_line_alert_df = pd.DataFrame()
        if(isExist):
            three_line_alert_df = pd.read_excel(three_line_file_name)

        isExist = os.path.exists(two_line_file_name)
        two_line_alert_df = pd.DataFrame()
        if(isExist):
            two_line_alert_df = pd.read_excel(two_line_file_name)

        if os.path.exists(data_store_file_name):
            with open(data_store_file_name, "r") as file:
                data_store = json.load(file)
        else:
            data_store = {}

        # max_execution_time = 5*3600
        logging.info(f'Stock threading stated ....')
        stock_data = pd.read_excel("stock market names.xlsx",sheet_name='Stock_list')
        is_history_starting_from,is_add_indicator=True,True
        # if time_frame in data_store and len(data_store[time_frame]) == len(stock_data):
        #     data_store[time_frame] = []
        thread_limit = 25
        total_rows = len(stock_data)
        threads = []
        # total_rows = 5
        # for start_index in range(0, 2, thread_limit):
        for start_index in range(0, total_rows, thread_limit):
            end_index = min(start_index + thread_limit, total_rows)
            for index in range(start_index, end_index):
                row = stock_data.iloc[index]
                thread = threading.Thread(target=generate_url, args=(row,time_frame,is_history_starting_from,is_add_indicator))
                thread.start()
                threads.append(thread)

            completed_thread = 0
            while len(threads) >= 5:
                for thread in threads:
                    if not thread.is_alive():
                        threads.remove(thread)
                        completed_thread += 1
            # for thread in threads:
                # thread.join()
            save_files()
            logging.info(f'{completed_thread } - thread Completed....')
            logging.info(f'{start_index} - {end_index} - Stock threading ended ....')
            end_time = time.time()
            elapsed_time = end_time - start_time
            if elapsed_time > max_execution_time:
                logging.info(f"Max time reached....")
                break
        for thread in threads:
            thread.join()
        save_files()
        if time_frame in data_store and len(data_store[time_frame]) >= len(stock_data):
            data_store[time_frame] = []
        else:
            # logging.info(f"PDF Generater Started...")
            # line_pattern_pdf_report.pdf_generater(time_frame,3)
            # logging.info(f"PDF Generater Ended...")
            try:
                logging.info(f"Yaml file editing Started...")
                # functions.yaml_file_edit(5,time_frame)
                logging.info(f"Yaml file editing Ended...")
            except Exception as e:
                logging.info(f"Yaml file editing file: {e}")
                traceback_msg = traceback.format_exc()
                logging.info(f"Error : {traceback_msg}")
            
            try:
                logging.info(f"Git push Started...")
                # functions.git_push(f'{ time_frame } yaml file run')
                logging.info(f"Git push Ended...")
            except Exception as e:
                logging.info(f"Git push : {e}")
                traceback_msg = traceback.format_exc()
                print(traceback_msg)
                logging.info(f"Error : {traceback_msg}")
    except Exception as e:
        logging.info(f"Error in main function: {e}")
        traceback_msg = traceback.format_exc()
        logging.info(f"Error : {traceback_msg}")

    try:
        three_line_alert_df.drop_duplicates(subset=['stockname','date1', 'row1', 'value1', 'date2', 'row2', 'value2', 'date3', 'row3', 'value3', 'buyORsell', 'slope', 'intercept','window_size','percentage_value','pivot_line_count'], keep='first', inplace=True)
        two_line_alert_df.drop_duplicates(subset=['stockname','date1', 'row1', 'value1', 'date2', 'row2', 'value2', 'buyORsell','window_size','percentage_value','two_line_count'], keep='first', inplace=True)
        three_line_alert_df.to_excel(three_line_file_name,index=False)
        two_line_alert_df.to_excel(two_line_file_name,index=False)
        logging.info(f'All stocks - alerts file Saved')
    except Exception as e:
        logging.info(f"Error in saving alerts file: {e}")
        traceback_msg = traceback.format_exc()
        logging.info(f"Error : {traceback_msg}")

    try:
        with open(data_store_file_name, "w") as file:
            json.dump(data_store, file,indent = 4)
        logging.info(f'All stocks - json file saved')
    except Exception as e:
        logging.info(f"Error in saving json file: {e}")
        traceback_msg = traceback.format_exc()
        logging.info(f"Error : {traceback_msg}")
    logging.info(f"Ended....")
    logging.info(f"PDF Generater Started...")
    functions.pdf_generater(time_frame,3)
    logging.info(f"PDF Generater Ended...")
