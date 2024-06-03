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
import sys
from fpdf import FPDF

import get_candle_data as get_candle_data
import functions
import telegram_message_send 

start_time = time.time()
max_execution_time = 5*3600
class pattern_detecter:
    def __init__(self, time_frame,window=10,percentage=1,pivot_line_count=3,two_line_count=2):
        self.lock = threading.Lock()

        os.makedirs(f"excel/{time_frame}", exist_ok=True)
        os.makedirs(f"json/", exist_ok=True)
        os.makedirs(f"log/", exist_ok=True)
        os.makedirs(f"stock_historical_data/{time_frame}", exist_ok=True)
        os.makedirs(f"pdf_report/", exist_ok=True)

        logging.basicConfig(filename=f'log/logfile_{time_frame}.log',level=logging.INFO, format='%(asctime)s -%(levelname)s - %(message)s')
        logging.info(f"Started...")

        self.percentage = percentage
        self.pivot_line_count  = pivot_line_count
        self.two_line_count = two_line_count

        self.time_frame = time_frame
        self.window=window
        self.three_line_file_name = f"excel/{self.time_frame}/three_line_alerts_{self.time_frame}.xlsx"
        self.two_line_file_name = f"excel/{self.time_frame}/two_line_alerts_{self.time_frame}.xlsx"
        self.ph_pl_data_file_name =  f"excel/{self.time_frame}/ph_pl_data_{self.time_frame}.xlsx"
        self.data_store_file_name = f"json/data_store_{self.time_frame}.json"

        self.three_line_file_name_backup = f"excel/{self.time_frame}/three_line_alerts_{self.time_frame}_backup.xlsx"
        self.two_line_file_name_backup = f"excel/{self.time_frame}/two_line_alerts_{self.time_frame}_backup.xlsx"
        self.ph_pl_data_file_name_backup =  f"excel/{self.time_frame}/ph_pl_data_{self.time_frame}_backup.xlsx"
        self.data_store_file_name_backup =  f"json/data_store_{self.time_frame}_backup.json"

        self.read_excel_file()

    def read_excel_file(self):

        # three line file
        isExist = os.path.exists(self.three_line_file_name)
        self.three_line_alert_df = pd.DataFrame()
        if(isExist):
            try:
                self.three_line_alert_df = pd.read_excel(self.three_line_file_name)
            except Exception as e:
                logging.info(f"reading {self.three_line_file_name} Error ")

        # three line file backup
        isExist = os.path.exists(self.three_line_file_name_backup)
        if(isExist):
            try:
                self.three_line_alert_df = pd.read_excel(self.three_line_file_name_backup)
            except Exception as e:
                logging.info(f"reading {self.three_line_file_name_backup} Error ")

        # two line file
        isExist = os.path.exists(self.two_line_file_name)
        self.two_line_alert_df = pd.DataFrame()
        if(isExist):
            try:
                self.two_line_alert_df = pd.read_excel(self.two_line_file_name)
            except Exception as e:
                logging.info(f"reading {self.two_line_file_name} Error ")

        # two line file backup
        isExist = os.path.exists(self.two_line_file_name_backup)
        if(isExist):
            try:
                self.two_line_alert_df = pd.read_excel(self.two_line_file_name_backup)
            except Exception as e:
                logging.info(f"reading {self.two_line_file_name_backup} Error ")
        
        # PH PL file
        isExist = os.path.exists(self.ph_pl_data_file_name)
        self.ph_pl_data_df = pd.DataFrame()
        if(isExist):
            try:
                self.ph_pl_data_df = pd.read_excel(self.ph_pl_data_file_name)
            except Exception as e:
                logging.info(f"reading {self.ph_pl_data_file_name} Error ")

        # PH PL file backup
        isExist = os.path.exists(self.ph_pl_data_file_name_backup)
        if(isExist):
            try:
                self.ph_pl_data_df = pd.read_excel(self.ph_pl_data_file_name_backup)
            except Exception as e:
                logging.info(f"reading {self.ph_pl_data_file_name_backup} Error ")


        self.data_store = {}
        if os.path.exists(self.data_store_file_name):
            with open(self.data_store_file_name, "r") as file:
                self.data_store = json.load(file)
        else:
            self.data_store[self.time_frame] = []
            self.data_store["completed"] = [0,0]

    def save_excel_file(self):
        try:
            self.three_line_alert_df.drop_duplicates(subset=['stockname','date1','value1', 'date2', 'value2', 'date3', 'value3', 'buyORsell'], keep='first', inplace=True)
            self.two_line_alert_df.drop_duplicates(subset=['stockname','date1','value1', 'date2','value2', 'buyORsell'], keep='first', inplace=True)
            self.ph_pl_data_df.drop_duplicates(inplace=True)

            self.three_line_alert_df.to_excel(self.three_line_file_name,index=False)
            self.two_line_alert_df.to_excel(self.two_line_file_name,index=False)
            self.ph_pl_data_df.to_excel(self.ph_pl_data_file_name,index=False)
            
            self.three_line_alert_df.to_excel(self.three_line_file_name_backup,index=False)
            self.two_line_alert_df.to_excel(self.two_line_file_name_backup,index=False)
            self.ph_pl_data_df.to_excel(self.ph_pl_data_file_name_backup,index=False)
            logging.info(f'All stocks - Excel file Saved')
        except Exception as e:
            logging.info(f"Error in saving alerts file: {e}")
            traceback_msg = traceback.format_exc()
            logging.info(f"Error : {traceback_msg}")
            

        try:
            with open(self.data_store_file_name, "w") as file:
                json.dump(self.data_store, file,indent = 4)

            with open(self.data_store_file_name_backup, "w") as file:
                json.dump(self.data_store, file,indent = 4)
            logging.info(f'All stocks - json file saved')
        except Exception as e:
            logging.info(f"Error in saving json file: {e}")
            traceback_msg = traceback.format_exc()
            logging.info(f"Error : {traceback_msg}")

    def add_ph_pl_values(self,stock_df, stock_name,number_of_datas=3):
        logging.info(f'{stock_name} - add_ph_pl_values function started')
        with self.lock:
            if(not self.ph_pl_data_df.empty):
                self.ph_pl_data_df = self.ph_pl_data_df[self.ph_pl_data_df['stockname'] != stock_name].copy()
                
            for pivot_value in [1,2]:    # high, low    
                df = stock_df[stock_df['isPivot'] == pivot_value].tail(number_of_datas)  #high
                if df.empty:
                    continue
                phorplvalues = df.apply(lambda row: row['High'] if row['isPivot'] == 1 else row['Low'], axis=1)
                highorlow = df['isPivot'].apply(lambda x: 'High' if x == 1 else 'Low')    
                new_data = {
                    "stockname" : stock_name,
                    "Datetime" : df["Datetime"],
                    # "Open" : df["Open"],
                    "High" : df["High"],
                    "Low" : df["Low"],
                    "Close" : df["Close"],
                    "isPivot" : df["isPivot"],
                    "PHorPLValue" : phorplvalues.tolist(),
                    # "HighorLow" : highorlow.tolist()
                }
                self.ph_pl_data_df = pd.concat([self.ph_pl_data_df,pd.DataFrame(new_data)])
                self.ph_pl_data_df.drop_duplicates(inplace=True)
            logging.info(f'{stock_name} - add_ph_pl_values function Ended')

    def process_row(self,candles,stock_name,function_name,number_of_calls=0):

        logging.info(f'{stock_name} - {"last n" if number_of_calls !=0 else ""} {function_name.__name__} function started')
        if(number_of_calls==0):
            candles[f'{function_name.__name__}'] = candles.apply(lambda row: function_name(candles,stock_name,
                                                                row.name, backcandles=15,
                                                                ), axis=1)
        else:
            candles['backup'] = candles[f'{function_name.__name__}']
            candles[f'{function_name.__name__}'] = candles.shift(-number_of_calls).iloc[-number_of_calls:].apply(
                                                        lambda row: function_name(candles,stock_name,
                                                        row.name, backcandles=15,
                                                        ), axis=1)
            candles[f'{function_name.__name__}'] = candles[f'{function_name.__name__}'].fillna(candles['backup'])
        logging.info(f'{stock_name} - {"last n" if number_of_calls !=0 else ""} {function_name.__name__} function Ended')


    def process_function(self,stock_df,stock_name,file_name,is_history_starting_from=True):

        try:
            logging.info(f'{stock_name} - Process function started')
            stock_data_historical = pd.DataFrame()  # Initialize with an empty DataFrame
            isExist = os.path.exists(file_name)
            if isExist:
                try:
                    logging.info(f"{stock_name} - Reading existing data...")
                    stock_data_historical = pd.read_excel(file_name)
                    is_history_starting_from = False
                except Exception as e:
                    print(f"Reading in File {stock_name}: {e}")
            
            stock_df = pd.concat([stock_data_historical, stock_df], axis=0)
            stock_df["Datetime"] = pd.to_datetime(stock_df["Datetime"], format='%d-%m-%Y %H:%M:%S')
            stock_df = stock_df.drop_duplicates(subset=['Datetime'], keep='first')\
                    .sort_values(by='Datetime')\
                    .reset_index(drop=True)

            number_of_calls = stock_df.isnull().any(axis=1).idxmax()
            if(number_of_calls==0 and is_history_starting_from):
                number_of_calls = 0
                logging.info(f'{stock_name} - isPivot function started')
                stock_df['isPivot'] = stock_df.apply(lambda row: self.isPivot(stock_df, stock_name, row.name), axis=1)
                logging.info(f'{stock_name} - isPivot function Ended')
                threads = []
                for function_name in [self.detect_structure, self.two_line_structure]:
                    thread = threading.Thread(target=self.process_row, args=(stock_df, stock_name, function_name,number_of_calls))
                    threads.append(thread)
                    thread.start()
                for thread in threads:
                    thread.join()
            elif(number_of_calls != 0):
                number_of_calls = max(0,number_of_calls - 50)
                logging.info(f'last n isPivot function started')
                stock_df['backup'] = stock_df['isPivot']
                stock_df['isPivot'] = stock_df.shift(-number_of_calls).iloc[-number_of_calls:].apply(lambda row: self.isPivot(stock_df, stock_name, row.name), axis=1)
                stock_df['isPivot'] = stock_df['isPivot'].fillna(stock_df['backup'])
                logging.info(f'last n isPivot function Ended')
                threads = []
                for function_name in [self.detect_structure, self.two_line_structure]:
                    thread = threading.Thread(target=self.process_row, args=(stock_df, stock_name, function_name,number_of_calls))
                    threads.append(thread)
                    thread.start()
                for thread in threads:
                    thread.join()
            self.add_ph_pl_values(stock_df.copy(), stock_name,3)
            self.data_store[self.time_frame].append(stock_name)
            stock_df.to_excel(file_name, index=False)

            self.save_excel_file()
            logging.info(f'{stock_name} - alerts file Saved')
            logging.info(f'{stock_name} - Process function Ended')
        except Exception as e:
            logging.info(f"{stock_name} - Error in process_function function: {e}")
            traceback_msg = traceback.format_exc()
            logging.info(f"{stock_name} - Error : {traceback_msg}")

    def generate_url_yfinance(self,stock_list, is_history_starting_from=False, is_add_indicator=True):
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        if elapsed_time > max_execution_time:
            logging.info(f"Max time reached....")
            return

        if self.time_frame in self.data_store:
            already_done = list(set(self.data_store[self.time_frame]).intersection(stock_list))
            for stock_name in already_done:
                logging.info(f"{stock_name} - this stock already completed")
            stock_list = list(set(stock_list) - set(self.data_store[self.time_frame]))
        
        try:
            if(stock_list):
                stocks_data_df = get_candle_data.get_candle_data_from_yfinance(tickers = stock_list, period='max', interval=self.time_frame)
                threads = []
                for stock_name in stocks_data_df:
                    print(stock_name)
                    stock_df = stocks_data_df[stock_name]
                    try:
                            file_name = f"stock_historical_data/{self.time_frame}/{stock_name}.xlsx"
                            
                            thread = threading.Thread(target=self.process_function, args=(stock_df,stock_name,file_name,))
                            threads.append(thread)
                            thread.start()
                            
                    except Exception as e:
                            logging.info(f"{stock_name} - Error in isPivot function: {e}")
                            traceback_msg = traceback.format_exc()
                            logging.info(f"{stock_name} - Error : {traceback_msg}")
                for thread in threads:
                    thread.join()
        except requests.RequestException as e:
            logging.info(f"{stock_name} - An error occurred: {e}")
            traceback_msg = traceback.format_exc()
            logging.info(f"{stock_name} - Error : {traceback_msg}")

        self.save_excel_file()

    def isPivot(self,df,stock_name,candle):
        """
        function that detects if a candle is a pivot/fractal point
        args: candle index, window before and after candle to test if pivot
        returns: 1 if pivot High, 2 if pivot Low, 3 if both and 0 default
        """
        try:
            if candle - self.window < 0 or candle + self.window >= len(df):
                return 0
            
            candle_data = df.iloc[candle]
            window_data = df.iloc[candle - self.window:candle + self.window + 1]

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
    
    def point_position_relative_to_line(self,df, point1, point2):

        x1, y1 = point1
        x2, y2 = point2
        
        if x2 == x1:
            return 0,0,1,1
        
        if x1 > x2:
            x1, y1, x2, y2 = x2, y2, x1, y1

        m = (y2 - y1) / (x2 - x1)
        c = y1 - m * x1

        subset_df = df.iloc[x1:x2].copy()
        # subset_df['line_y'] = m * subset_df.index + c
        subset_df.loc[:,'line_y'] = m * subset_df.index + c
        subset_df = subset_df.dropna(subset=['Close', 'line_y'])

        above_count = (subset_df['Close'] > subset_df['line_y']).sum()
        below_count = (subset_df['Close'] < subset_df['line_y']).sum()
        
        total = above_count + below_count
        if total == 0:
            return above_count, below_count, 1,1
        return above_count, below_count,above_count/total,below_count/total

    def plus_minus_01_percent(self,x_values,y_values):
        try:
            # x_values,y_values = zip(*combination)
            slope, intercept, _, _, _ = linregress(x_values[0:2], y_values[0:2])
            predicted_y_value = slope * x_values[-1] + intercept
            actual_y_value = y_values[-1]

            percent_difference = abs(predicted_y_value - actual_y_value) / actual_y_value * 100
            # print(percent_difference,percentage)
            return slope, intercept,percent_difference <= self.percentage

        except Exception as e:
            logging.info(f"{x_values,y_values} - Error in Plus minus 1 percents function: {e}")
            traceback_msg = traceback.format_exc()
            logging.info(f"{x_values,y_values} - message : {traceback_msg}")

    def detect_structure(self,df,stockname,candle, backcandles):
        if (candle <= (backcandles+self.window)) and (candle+self.window+1 >= len(df)):
            return 0
        localdf = df.iloc[0:candle-self.window] 
        Highs = localdf[localdf['isPivot'] == 1].High.tail(5).values
        x_values_Highs = localdf[localdf['isPivot'] == 1].index[-5:].values
        Highs_values = pd.DataFrame(localdf[localdf['isPivot'] == 1].High.tail(5))

        Lows = localdf[localdf['isPivot'] == 2].Low.tail(5).values
        x_values_Lows = localdf[localdf['isPivot'] == 2].index[-5:].values
        Lows_values = pd.DataFrame(localdf[localdf['isPivot'] == 2].Low.tail(5))
        levelbreak = 0
        if Lows_values.shape[0]>=self.pivot_line_count and candle-self.window-1 == Lows_values.index[-1]:
            combinations = list(itertools.combinations(zip(x_values_Lows, Lows), self.pivot_line_count))
            leatest_combinations = [[(index_low[0], index_low[1]) for index_low in combination] for combination in combinations]
            for combination in leatest_combinations:
                x_values,y_values = zip(*combination)
                slope, intercept , is_line= self.plus_minus_01_percent(x_values,y_values)

                point1 = x_values[0],y_values[0]
                point2 =x_values[-1],y_values[-1]
                above_count, below_count,above_percentage,\
                    below_percentage = self.point_position_relative_to_line(localdf,point1,point2)
                if(is_line and below_percentage < 0.06):
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
                        'date3': [df.iloc[alert.index[2]].Datetime] if self.pivot_line_count>2 else 0,
                        'row3': [alert.index[2]] if self.pivot_line_count>2 else 0,
                        'value3': [alert.iloc[2, 0]] if self.pivot_line_count>2 else 0,
                        'buyORsell' : 'Low' if levelbreak==1 else 'High',
                        "slope" : slope,
                        "intercept" : intercept,
                        "window_size":self.window,
                        "percentage_value" : self.percentage,
                        "pivot_line_count" : self.pivot_line_count
                    })
                    self.three_line_alert_df = pd.concat([self.three_line_alert_df, row_to_append], ignore_index=True)
        if Highs_values.shape[0]>=self.pivot_line_count and candle-self.window-1 == Highs_values.index[-1]:
            combinations = list(itertools.combinations(zip(x_values_Highs, Highs), self.pivot_line_count))
            leatest_combinations = [[(index_high[0], index_high[1]) for index_high in combination] for combination in combinations]
            for combination in leatest_combinations:
                x_values,y_values = zip(*combination)
                slope, intercept , is_line= self.plus_minus_01_percent(x_values,y_values)

                point1 = x_values[0],y_values[0]
                point2 =x_values[-1],y_values[-1]
                above_count, below_count,above_percentage,\
                    below_percentage = self.point_position_relative_to_line(localdf,point1,point2)
                if(is_line and above_percentage < 0.06):
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
                        'date3': [df.iloc[alert.index[2]].Datetime] if self.pivot_line_count>2 else 0,
                        'row3': [alert.index[2]] if self.pivot_line_count>2 else 0,
                        'value3': [alert.iloc[2, 0]] if self.pivot_line_count>2 else 0,
                        'buyORsell' : 'Low' if levelbreak==1 else 'High',
                        "slope" : slope,
                        "intercept" : intercept,
                        "window_size":self.window,
                        "percentage_value" : self.percentage,
                        "pivot_line_count" : self.pivot_line_count
                    })
                    self.three_line_alert_df = pd.concat([self.three_line_alert_df, row_to_append], ignore_index=True)

        return levelbreak

    def two_line_structure(self,df,stockname,candle, backcandles):
        if (candle <= (backcandles+self.window)) and (candle+self.window+1 >= len(df)):
            return 0
        localdf = df.iloc[0:candle-self.window] 
        Highs = localdf[localdf['isPivot'] == 1].High.tail(3).values
        x_values_Highs = localdf[localdf['isPivot'] == 1].index[-3:].values
        Highs_values = pd.DataFrame(localdf[localdf['isPivot'] == 1].High.tail(3))

        Lows = localdf[localdf['isPivot'] == 2].Low.tail(3).values
        x_values_Lows = localdf[localdf['isPivot'] == 2].index[-3:].values
        Lows_values = pd.DataFrame(localdf[localdf['isPivot'] == 2].Low.tail(3))
        levelbreak = 0
        if Lows_values.shape[0]>=self.two_line_count and candle-self.window-1 == Lows_values.index[-1]:
            combinations = list(itertools.combinations(zip(x_values_Lows, Lows), self.two_line_count))
            leatest_combinations = [[(index_low[0], index_low[1]) for index_low in combination] for combination in combinations]
            for combination in leatest_combinations:
                x_values,y_values = zip(*combination)
                percent_difference = abs(y_values[0] - y_values[1]) / y_values[1] * 100
                is_line = percent_difference<=self.percentage
                slope, intercept , _= self.plus_minus_01_percent(x_values,y_values)

                point1 = x_values[0],y_values[0]
                point2 =x_values[1],y_values[1]
                above_count, below_count,above_percentage,\
                    below_percentage = self.point_position_relative_to_line(localdf,point1,point2)
                if(is_line and below_percentage < 0.06):
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
                        "slope" : slope,
                        "intercept" : intercept,
                        "window_size":self.window,
                        "percentage_value" : self.percentage,
                        "two_line_count" : self.two_line_count
                    })
                    self.two_line_alert_df = pd.concat([self.two_line_alert_df, row_to_append], ignore_index=True)

        if Highs_values.shape[0]>=self.two_line_count and candle-self.window-1 == Highs_values.index[-1]:
            combinations = list(itertools.combinations(zip(x_values_Highs, Highs), self.two_line_count))
            leatest_combinations = [[(index_high[0], index_high[1]) for index_high in combination] for combination in combinations]
            for combination in leatest_combinations:
                x_values,y_values = zip(*combination)
                percent_difference = abs(y_values[0] - y_values[1]) / y_values[1] * 100
                is_line = percent_difference<=1
                slope, intercept , _= self.plus_minus_01_percent(x_values,y_values)
                point1 = x_values[0],y_values[0]
                point2 =x_values[1],y_values[1]
                above_count, below_count,above_percentage,\
                    below_percentage = self.point_position_relative_to_line(localdf,point1,point2)
                if(is_line and above_percentage < 0.06):
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
                        "slope" : slope,
                        "intercept" : intercept,
                        "window_size":self.window,
                        "percentage_value" : self.percentage,
                        "two_line_count" : self.two_line_count
                    })
                    self.two_line_alert_df = pd.concat([self.two_line_alert_df, row_to_append], ignore_index=True)
        return levelbreak

if __name__=="__main__":
    try:
        time_frame,window = "1d",10
        if len(sys.argv) >= 2:
            time_frame = sys.argv[1]
            if len(sys.argv) >= 3:
                window = sys.argv[2]

            
        stock_data = pd.read_excel("stock market names.xlsx",sheet_name='Stock_list')
        is_history_starting_from,is_add_indicator=True,True

        thread_limit,total_rows = 25,len(stock_data)
        # thread_limit,total_rows = 25,10
        input_json_file = "input.json"
        with open(input_json_file, "r") as file:
            input_data = json.load(file)
        
        window,percentage,pivot_line_count,two_line_count= input_data[time_frame]["window"],input_data[time_frame]["percentage"], \
                                                        input_data[time_frame]["pivot_line_count"],input_data[time_frame]["two_line_count"]
        pattern_detecter_obj = pattern_detecter(time_frame,window,percentage,pivot_line_count,two_line_count)
        print(time_frame,window,percentage,pivot_line_count,two_line_count)
        time.sleep(1000)
        stock_status = pattern_detecter_obj.data_store['completed']
        itr_completed,index = stock_status
        all_stock_name_list = stock_data.iloc[:]['YFINANCE'].tolist()#[:5]
        thread_limit,total_rows = 25,len(all_stock_name_list)
        # for itr in range(itr_completed,2):
        itr = itr_completed
        while(len(all_stock_name_list)>0 and itr <= 10):
            # total_rows += 5*itr
            if(itr > 5):
                all_stock_name_list = [name.split('.')[0] + '.NS' if name.endswith('.BO') else name.split('.')[0] + '.BO' for name in all_stock_name_list]
            thread_limit,total_rows = 25,len(all_stock_name_list)
            logging.info(f"Itr - {itr} - thread_limit - {thread_limit}  - total_rows - {total_rows}")
            for start_index in range(index, total_rows, thread_limit):
                end_index = min(start_index + thread_limit, total_rows)
                thread_stock_name_list = []
                for index in range(start_index, end_index):
                    # stock_name = stock_data.iloc[index]['YFINANCE']
                    stock_name = all_stock_name_list[index]
                    thread_stock_name_list.append(stock_name)
                # stock_name_list = ["DMART.NS"]
                pattern_detecter_obj.generate_url_yfinance(thread_stock_name_list,is_history_starting_from,is_add_indicator)
                pattern_detecter_obj.data_store['completed'][1] = end_index
                pattern_detecter_obj.save_excel_file()
                logging.info(f'{start_index} - {end_index} - Stock threading ended ....')

                end_time = time.time()
                elapsed_time = end_time - start_time
                if elapsed_time > max_execution_time:
                    logging.info(f"Max time reached....")
                    # telegram_message_send.send_message_with_documents(message=f"Max time reached..{ time_frame}")
                    break

            else:
                pattern_detecter_obj.data_store['completed'][0] +=1
                logging.info(f"{itr} - time Completed ")
                # break
            
            all_stock_name_list = list(set(all_stock_name_list) - set(pattern_detecter_obj.data_store[time_frame]))
            end_time = time.time()
            elapsed_time = end_time - start_time
            if elapsed_time > max_execution_time:
                logging.info(f"Max time reached....")
                telegram_message_send.send_message_with_documents(message=f"""Max time reached..{ time_frame} 
                                                                  no. {len(all_stock_name_list)} pending stock""")
                break
            itr +=1
        else:
            logging.info(f"All sttock completed...")
            logging.info(f"After {itr} itr Completed...Exit...")
            logging.info(f"Reset to default value..")

            # stock_name_list = stock_data.iloc[:]['YFINANCE']
            stock_not_tested = list(set(all_stock_name_list) - set(pattern_detecter_obj.data_store[time_frame]))
            if stock_not_tested:
                not_in_list_df = pd.DataFrame(stock_not_tested,columns=["Stocks"])
                pdf = FPDF(unit='mm', format=(270, 297))
                pdf.add_page()
                pdf.set_font('Arial', 'B', 16)

                functions.output_df_to_pdf("List of Stock not Tested after Two time ",pdf,not_in_list_df)
                current_time = datetime.datetime.now()
                date_time = current_time.strftime("%Y%m%d.%H%M%S")
                os.makedirs(f"pdf_report/stock_not_tested/", exist_ok=True)
                pdf_name = f'pdf_report/stock_not_tested/stock_not_tested_{time_frame}_{date_time}.pdf'
                pdf.output(pdf_name, 'F')
                telegram_message_send.send_message_with_documents( #message="stock not tested",
                                                                  document_paths=[pdf_name],
                                                                  captions=[f"stock not tested -{len(stock_not_tested)} {time_frame}"])

            pattern_detecter_obj.data_store[time_frame] = []
            pattern_detecter_obj.data_store["completed"] = [0,0]
        pattern_detecter_obj.save_excel_file()
        
    except Exception as e:
        pattern_detecter_obj.save_excel_file()
        logging.info(f"Error in main function: {e}")
        traceback_msg = traceback.format_exc()
        logging.info(f"Error : {traceback_msg}")
