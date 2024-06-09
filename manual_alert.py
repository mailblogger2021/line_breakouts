import requests
import pandas as pd
import time
import datetime
import os
import get_candle_data 
import logging
import traceback
import json

def load_excel(file_name):
    if os.path.exists(file_name):
        df = pd.read_excel(file_name)
    else:
        df = pd.DataFrame(columns=['ACTION', 'STOCK_NAME', 'HIGHorLOW', 'PRICE', 'STATUS', 'LAST_UPDATED','NOTES'])
    return df

def save_excel(df, file_name):
    df.drop_duplicates(subset=['ACTION','STOCK_NAME', 'HIGHorLOW', 'PRICE'], keep='first', inplace=True)
    df.to_excel(file_name, index=False)
    return df

def add_to_excel(df, message):
    if len(message) < 4:
        return df
    action, stock_name, HighorLow, price = map(str.strip, message)
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        if action == 'ADD':
            new_data = {
                'ACTION': action,
                'STOCK_NAME': stock_name,
                'HIGHorLOW': HighorLow,
                'PRICE': float(price),
                'STATUS': 'Pending',
                'LAST_UPDATED': current_time,
                'NOTES' : 'New data added'
            }
            df = pd.concat([df, pd.DataFrame([new_data])], ignore_index=True)
            # print(f"Stock {stock_name} added with price {price}.")
        elif action == 'DONE':
            if stock_name in df['STOCK_NAME'].values:
                df.loc[(df['ACTION'] != action) & (df['STOCK_NAME'] == stock_name) & (df['HIGHorLOW'] == HighorLow) & (df['PRICE'] == float(price)), 'ACTION'] = action
                df.loc[(df['ACTION'] != action) & (df['STOCK_NAME'] == stock_name) & (df['HIGHorLOW'] == HighorLow) & (df['PRICE'] == float(price)), 'STATUS'] = 'Done'
                df.loc[(df['ACTION'] != action) & (df['STOCK_NAME'] == stock_name) & (df['HIGHorLOW'] == HighorLow) & (df['PRICE'] == float(price)), 'LAST_UPDATED'] = current_time
            else:
                print(f"Stock {stock_name} not found.")
        else:
            print("Invalid action. Expected 'ADD' or 'DONE'.")
    except Exception as e:
        print(e)
    return df

def send_telegram_message(bot_token, chat_id, message):
    url = f"https://api.telegram.org/{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending message to Telegram: {e}")

def manual_breakout_check(breakout_file_name,alert_data):
    break_out_stocks = pd.DataFrame()
    time_frame = "1d"
        
    if os.path.exists(breakout_file_name):
        manual_data = pd.read_excel(breakout_file_name)
        if manual_data.empty:
            return
        stock_names = manual_data[manual_data['ACTION'] != 'DONE']['STOCK_NAME'].unique().tolist()

        stock_all_df = get_candle_data.get_candle_data_from_yfinance(tickers=stock_names, period='max', interval=time_frame)

        for stock_name in stock_names:
            if stock_name not in stock_all_df:
                continue
            try:
                stock_manual_alert = manual_data[manual_data['STOCK_NAME'] == stock_name]
                last_stock_df = stock_all_df[stock_name]
                
                last_stock_df = last_stock_df.sort_index()
                previous_date,current_date = list(last_stock_df['Datetime'].tail(2))
                previous,current = list(last_stock_df['Close'].tail(2))
                new_break_out_stocks = stock_manual_alert[
                    (stock_manual_alert['ACTION'] == 'ADD' ) &
                    (previous < stock_manual_alert['PRICE'] ) & 
                    (stock_manual_alert['PRICE'] < current) & 
                    (stock_manual_alert['HIGHorLOW'] == 'HIGH')
                    ].copy()
                
                if(not new_break_out_stocks.empty):
                    new_break_out_stocks.loc[:, "TdyDate"] = current_date
                    new_break_out_stocks.loc[:, "TdyClose"] = current
                    new_break_out_stocks.loc[:, "PClose"] = previous
                    break_out_stocks = pd.concat([break_out_stocks, new_break_out_stocks], ignore_index=True)

                    for _, row in new_break_out_stocks.iterrows():
                        message = f"High Breakout Alert\n{row['STOCK_NAME']}: Price {current:.2f} crossed {row['PRICE']:.2f}\nPrevious Price: {previous:.2f}"
                        send_telegram_message(bot_token, chat_id, message)
                        done_message = f"DONE,{row['STOCK_NAME']},{row['HIGHorLOW']},{row['PRICE']}"
                        alert_data = add_to_excel(alert_data, done_message.split(','))


                new_break_out_stocks = stock_manual_alert[
                    (stock_manual_alert['ACTION'] == 'ADD' ) &
                    (stock_manual_alert['PRICE'] < previous ) & 
                    (current < stock_manual_alert['PRICE']) & 
                    (stock_manual_alert['HIGHorLOW'] == 'LOW')
                    ].copy()
                
                if(not new_break_out_stocks.empty):
                    new_break_out_stocks.loc[:, "TdyDate"] = current_date
                    new_break_out_stocks.loc[:, "TdyClose"] = current
                    new_break_out_stocks.loc[:, "PClose"] = previous
                    break_out_stocks = pd.concat([break_out_stocks, new_break_out_stocks], ignore_index=True)

                    for _, row in new_break_out_stocks.iterrows():
                        message = f"Low Breakout Alert\n{row['STOCK_NAME']}: Price {current:.2f} fell below {row['PRICE']:.2f}\nPrevious Price: {previous:.2f}"
                        send_telegram_message(bot_token, chat_id, message)
                        done_message = f"DONE,{row['STOCK_NAME']},{row['HIGHorLOW']},{row['PRICE']}"
                        alert_data = add_to_excel(alert_data, done_message.split(','))
                        
            except Exception as e:
                logging.info(f"{stock_name} Error: {e}")
                traceback_msg = traceback.format_exc()
                logging.info(f"Error: {traceback_msg}")
    return break_out_stocks,alert_data

if __name__=="__main__":

    input_json_file = "data_store_input.json"
    input_data,last_message_time = {},0
    if os.path.exists(input_json_file):
        with open(input_json_file, "r") as file:
            input_data = json.load(file)

        if 'telegram_last_message' in input_data:
            last_message_time = input_data['telegram_last_message']
        else:
            last_message_time = 0
    time_frame = "1d"
    os.makedirs(f"log/", exist_ok=True)
    logging.basicConfig(filename=f'log/manual_alert{",".join(time_frame)}.log',level=logging.INFO, format='%(asctime)s -%(levelname)s - %(message)s')
    logging.info(f"Started...")

    bot_token = "bot6511501073:AAHbWvFY_dKcUQfKNGFODOeYK8PEUJ4vXPI"
    chat_id = -1002240464638
    url = f"https://api.telegram.org/{bot_token}/getUpdates"
    response = requests.get(url)
    
    if response.status_code == 200:
        os.makedirs(f"excel/breakout/", exist_ok=True)
        file_name ='excel/breakout/manual_alert.xlsx'
        df = load_excel(file_name)
        data = response.json()
        for result in data['result']:
            if(result['message']['chat']['id'] == -1002240464638):
                if 'text' in result['message']:
                    if 'date' in result['message']:
                        message_epoch_timestamp = result['message']['date']
                        current_epoch_timestamp = int(time.time())
                        if (message_epoch_timestamp > last_message_time):
                            message = result['message']['text'].upper().split(',')
                            print(message)
                            df = add_to_excel(df, message)
                        input_data['telegram_last_message'] = message_epoch_timestamp
        df = save_excel(df, file_name)
        
        with open(input_json_file, "w") as file:
            json.dump(input_data,file,indent = 4)

        break_out_stocks,df = manual_breakout_check(file_name, df)
        save_excel(df, file_name)

    else:
        print(f"Failed to retrieve data: {response.status_code}")



    