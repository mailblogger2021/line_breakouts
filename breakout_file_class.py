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
import get_candle_data as get_candle_data

class StockAnalyzer:
    
    def __init__(self, window=21, percentage=1, pivot_line_count=3, alert_file_name="alerts.xlsx"):
        self.window = window
        self.percentage = percentage
        self.pivot_line_count = pivot_line_count
        self.alert_file_name = alert_file_name
        self.alerts = pd.DataFrame()
        self.load_alerts()

    def load_alerts(self):
        if os.path.exists(self.alert_file_name):
            self.alerts = pd.read_excel(self.alert_file_name)

    def generate_url(self, rows, time_frame, is_history_starting_from=False, is_add_indicator=True):
        stock_name = rows['STOCK NAME']
        index_name = rows['INDEX NAME']
        stock_instrument_token = rows['INSTRUMENT_TOKEN']
        fno = rows['FNO']
        logging.info(f"{stock_name} - kite url call started...")
        print(stock_name, index_name, stock_instrument_token, fno)
        
        # Make the request using a session
        with requests.Session() as session:
            candles = get_candle_data.get_kite_url(session, rows, time_frame, is_history_starting_from=is_history_starting_from, is_add_indicator=is_add_indicator)
        
        logging.info(f"{stock_name} - kite url call ended...")
        
        # Save candles data to Excel file
        candles.to_excel(f".stock_historical_data/{time_frame}/{stock_name}.xlsx", index=False)
        
        # Process candles data
        self.preparing_for_candles(candles, stock_name)
        session.close()

    def isPivot(self, df, stock_name, candle, window):
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

    def plus_minus_01_percent(self, combination, percentage):
        try:
            x_values,y_values = zip(*combination)
            slope, intercept, _, _, _ = linregress(x_values[0:2], y_values[0:2])
            predicted_y_value = slope * x_values[-1] + intercept
            actual_y_value = y_values[-1]
            # print(x_values[-1],y_values[-1],x_values[0:2], y_values[0:2])
            # Calculate 0.1% of the value
            # percent_01 = percentage / 100 * actual_y_value  # 0.1

            percent_difference = abs(predicted_y_value - actual_y_value) / actual_y_value * 100
            # print(percent_difference,percentage)
            return slope, intercept,percent_difference <= percentage

            print(slope, intercept,plus_value,minus_value,actual_y_values,predicted_y_values)
            print(predicted_y_values>minus_value and predicted_y_values < plus_value)
            return slope, intercept,predicted_y_values>minus_value and predicted_y_values < plus_value
        except Exception as e:
            logging.info(f"Error in Plus minus 1 percents function: {e}")

    def detect_structure(self, df, stock_name, candle, backcandles, window):
        """
        Attention! window should always be greater than the pivot window! to avoid look ahead bias
        """
        try:
            if (candle <= (backcandles+window)) and (candle+window+1 >= len(df)):
                # print(candle)
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
            global alerts
            if Lows_values.shape[0]>=self.pivot_line_count and candle-window-1 == Lows_values.index[-1]:
                combinations = list(itertools.combinations(zip(x_values_Lows, Lows), self.pivot_line_count))
                leatest_combinations = [[(index_low[0], index_low[1]) for index_low in combination] for combination in combinations]
                for combination in leatest_combinations:
                    slope, intercept , is_line= self.plus_minus_01_percent(combination,self.percentage)
                    if(is_line):
                        levelbreak = 1
                        alert = pd.DataFrame(combination, columns=['index', 'value']).set_index('index').copy(deep=True)
                        row_to_append = pd.DataFrame({
                            'stockname' : stock_name,
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
                            "window_size":window,
                            "percentage_value" : self.percentage,
                            "pivot_line_count" : self.pivot_line_count
                        })
                        alerts = pd.concat([alerts, row_to_append], ignore_index=True)
            if Highs_values.shape[0]>=self.pivot_line_count and candle-window-1 == Highs_values.index[-1]:
                combinations = list(itertools.combinations(zip(x_values_Highs, Highs), self.pivot_line_count))
                leatest_combinations = [[(index_high[0], index_high[1]) for index_high in combination] for combination in combinations]
                for combination in leatest_combinations:
                    slope, intercept , is_line= self.plus_minus_01_percent(combination,self.percentage)
                    if(is_line):
                        levelbreak = 2
                        alert = pd.DataFrame(combination, columns=['index', 'value']).set_index('index').copy(deep=True)
                        # print(alert)
                        row_to_append = pd.DataFrame({
                            'stockname' : stock_name,
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
                            "window_size":window,
                            "percentage_value" : self.percentage,
                            "pivot_line_count" : self.pivot_line_count
                        })
                        # global alerts
                        alerts = pd.concat([alerts, row_to_append], ignore_index=True)
                        # print(alerts)

            # alerts = alerts.drop_duplicates()
            alerts.drop_duplicates(subset=['stockname','date1', 'row1', 'value1', 'date2', 'row2', 'value2', 'date3', 'row3', 'value3', 'buyORsell', 'slope', 'intercept','window_size','percentage_value','pivot_line_count'], keep='last', inplace=True)
            return levelbreak
        except Exception as e:
            # print(e)
            logging.info(f"Error in Detect Structure function: {e}")

    def preparing_for_candles(self, df, stock_name):
        logging.info(f'{stock_name} - preparing_for_candles function started')
        logging.info(f'{stock_name} - isPivot function started')
        df['isPivot'] = df.apply(lambda row: self.isPivot(df,stock_name,row.name,self.window), axis=1)
        logging.info(f'{stock_name} - isPivot function Ended')

        logging.info(f'{stock_name} - detect_structure function started')
        df['pattern_detected'] = df.apply(lambda row: self.detect_structure(df,stock_name,
                                                                row.name, backcandles=20, window=self.window,
                                                                ), axis=1)
        logging.info(f'{stock_name} - detect_structure function Ended')
        global alerts
        # alerts = alerts.drop_duplicates()
        alerts.drop_duplicates(subset=['stockname','date1', 'row1', 'value1', 'date2', 'row2', 'value2', 'date3', 'row3', 'value3', 'buyORsell', 'slope', 'intercept','window_size','percentage_value','pivot_line_count'], keep='last', inplace=True)
        alerts.to_excel(alert_file_name,index=False)
        logging.info(f'{stock_name} - preparing_for_candles function Ended')
        logging.info(f'{stock_name} - alerts file Saved')

    def analyze_stocks(self, stock_data, time_frame="day", num_threads=10):
        try:
            logging.info(f'Stock threading started ....')
            threads = []
            is_history_starting_from, is_add_indicator = True, False
            for index, row in stock_data.iterrows():
                logging.info(f'{index} - started')
                thread = threading.Thread(target=self.generate_url, args=(row, time_frame, is_history_starting_from, is_add_indicator))
                thread.start()
                threads.append(thread)
                if index == num_threads:  # Limiting to a certain number of threads
                    break
            for thread in threads:
                thread.join()
            logging.info(f'Stock threading ended ....')
        except Exception as e:
            logging.info(f"Error in analyze_stocks method: {e}")

    def save_alerts(self):
        try:
            self.alerts.drop_duplicates(subset=['stockname','date1', 'row1', 'value1', 'date2', 'row2', 'value2', 'date3', 'row3', 'value3', 'buyORsell', 'slope', 'intercept','window_size','percentage_value','pivot_line_count'], keep='last', inplace=True)
            self.alerts.to_excel(self.alert_file_name, index=False)
            logging.info(f'All stocks - alerts file Saved')
        except Exception as e:
            logging.info(f"Error in saving alerts file: {e}")
