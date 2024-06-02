import yfinance as yf
import pandas as pd
import sys
import logging
import traceback
import os
import datetime
from fpdf import FPDF

from functions import output_df_to_pdf
import get_candle_data 
import telegram_message_send


all_stock_data = {}
def point_position_relative_to_line(df, point1, point2):

    x1, y1 = point1
    x2, y2 = point2

    if x1 > x2:
        x1, y1, x2, y2 = x2, y2, x1, y1

    m = (y2 - y1) / (x2 - x1)
    c = y1 - m * x1

    subset_df = df.iloc[x1:x2].copy()
    # subset_df['line_y'] = m * subset_df.index + c
    subset_df.loc[:,'line_y'] = m * subset_df.index + c

    above_count = (subset_df['Close'] > subset_df['line_y']).sum()
    below_count = (subset_df['Close'] < subset_df['line_y']).sum()
    
    return above_count, below_count,above_count/(above_count+below_count),below_count/(above_count+below_count)

def ph_pl_data_breakout(time_frames,breakout_file_name):
    break_out_stocks = pd.DataFrame()
    for time_frame in time_frames:
        print(time_frame)

        yfinance_time_frame = time_frames_for_yfinance[time_frame]
        
        df_file_name = breakout_file_name.format(yfinance_time_frame=yfinance_time_frame)
        
        if os.path.exists(df_file_name):
            ph_pl_stock_data = pd.read_excel(df_file_name)
            if ph_pl_stock_data.empty:
                continue
            stock_names = ph_pl_stock_data['stockname'].unique().tolist()

            # if yfinance_time_frame not in all_stock_data:
            #     all_stock_data[yfinance_time_frame] = {}

            # stocks_to_fetch = list(set(stock_names) - set(all_stock_data[yfinance_time_frame].keys()))
            # if stocks_to_fetch:
            stock_all_df = get_candle_data.get_candle_data_from_yfinance(tickers=stock_names, period='max', interval=yfinance_time_frame)
                # all_stock_data[yfinance_time_frame].update(new_data)
            # stock_all_df = {stock: all_stock_data[yfinance_time_frame][stock] for stock in stock_names}

            for stock_name in stock_names:
                if stock_name not in stock_all_df:
                    continue
                try:
                    stock_ph_pl_df = ph_pl_stock_data[ph_pl_stock_data['stockname'] == stock_name]
                    last_stock_df = stock_all_df[stock_name]
                    
                    last_stock_df = last_stock_df.sort_index()
                    previous_date,current_date = list(last_stock_df['Datetime'].tail(2))
                    previous,current = list(last_stock_df['High'].tail(2))
                    new_break_out_stocks = stock_ph_pl_df[
                        (previous < stock_ph_pl_df['High'] ) & (stock_ph_pl_df['High'] < current) & (stock_ph_pl_df['isPivot'] == 1)
                        ].copy()
                    
                    if(not new_break_out_stocks.empty):
                        new_break_out_stocks.loc[:, "isPivot"] = "High"
                        new_break_out_stocks.loc[:, "time_frame"] = time_frame
                        new_break_out_stocks.loc[:, "TdyDate"] = current_date
                        new_break_out_stocks.loc[:, "TdyClose"] = current
                        new_break_out_stocks.loc[:, "PClose"] = previous
                        break_out_stocks = pd.concat([break_out_stocks, new_break_out_stocks], ignore_index=True)

                    previous,current = list(last_stock_df['Low'].tail(2))
                    new_break_out_stocks = stock_ph_pl_df[
                        (stock_ph_pl_df['Low'] < previous ) & (current < stock_ph_pl_df['Low']) & (stock_ph_pl_df['isPivot'] == 2)
                        ].copy()
                    
                    if(not new_break_out_stocks.empty):
                        new_break_out_stocks.loc[:, "isPivot"] = "Low"
                        new_break_out_stocks.loc[:, "time_frame"] = time_frame
                        new_break_out_stocks.loc[:, "TdyDate"] = current_date
                        new_break_out_stocks.loc[:, "TdyClose"] = current
                        new_break_out_stocks.loc[:, "PClose"] = previous
                        break_out_stocks = pd.concat([break_out_stocks, new_break_out_stocks], ignore_index=True)
                            
                except Exception as e:
                    logging.info(f"{stock_name} Error : {e}")
                    traceback_msg = traceback.format_exc()
                    logging.info(f"Error : {traceback_msg}")
    return break_out_stocks

def stock_break_out_finder(time_frames,breakout_file_name,number_of_line="three"):
    break_out_stocks = pd.DataFrame()
    for time_frame in time_frames:
        print(time_frame)

        yfinance_time_frame = time_frames_for_yfinance[time_frame]
        
        df_file_name = breakout_file_name.format(yfinance_time_frame=yfinance_time_frame)
        
        if os.path.exists(df_file_name):
            stock_df = pd.read_excel(df_file_name)
            if stock_df.empty:
                continue
            stock_names = stock_df['stockname'].unique().tolist()

            # if yfinance_time_frame not in all_stock_data:
            #     all_stock_data[yfinance_time_frame] = {}

            # stocks_to_fetch = list(set(stock_names) - set(all_stock_data[yfinance_time_frame].keys()))
            # if stocks_to_fetch:
            stock_all_df = get_candle_data.get_candle_data_from_yfinance(tickers=stock_names, period='max', interval=yfinance_time_frame)
                # all_stock_data[yfinance_time_frame].update(new_data)
            # stock_all_df = {stock: all_stock_data[yfinance_time_frame][stock] for stock in stock_names}
            # stock_all_df = get_candle_data.get_candle_data_from_yfinance(tickers=stock_names.copy(), period='max', interval=yfinance_time_frame)

            # print(len(list(all_stock_data.keys())))
            # print(len(list(all_stock_data[yfinance_time_frame].keys())))
            last_five_df = stock_df.sort_values(by='alert_date').groupby('stockname').tail(5)
            for stock_name in stock_names:
                if stock_name not in stock_all_df:
                    continue
                try:
                    last_five_stock_df = last_five_df[last_five_df['stockname'] == stock_name]
                    last_stock_df = stock_all_df[stock_name]
                    # if(stock_name == 'DMART.NS'):
                        # print(stock_name)
                    # y = pd.read_excel(file_name)

                    last_five_stock_df = last_five_stock_df.copy()
                    # last_five_stock_df['Timestamp'] = pd.to_datetime(last_five_stock_df['alert_date'], format="%Y-%m-%d %H:%M:%S").apply(lambda z: int(z.timestamp()))
                    last_five_stock_df.loc[:, 'Timestamp'] = pd.to_datetime(last_five_stock_df['alert_date'], format="%Y-%m-%d %H:%M:%S").apply(lambda z: int(z.timestamp()))
                    last_stock_df['Timestamp'] = pd.to_datetime(last_stock_df['Datetime'], format="%d-%m-%Y %H:%M:%S").apply(lambda z: int(z.timestamp()))
                    
                    for index in range(-1,-len(last_five_stock_df),-1):
                        merge_data = last_stock_df[last_stock_df['Timestamp'] == last_five_stock_df['Timestamp'].iloc[index]]
                        if(len(merge_data)>0):
                            target_row = merge_data.index[0]
                            new_index_value = last_five_stock_df['rowNumber'].iloc[index]
                            break
                    else:
                        continue

                    new_index = list(range(new_index_value - target_row, new_index_value)) + \
                                [new_index_value] + \
                                list(range(new_index_value + 1, new_index_value + len(last_stock_df) - target_row))
                    last_stock_df.index = new_index
                    y_last_two = last_stock_df[-2:]

                    # last two index and Close price  ( yesterday and today values )
                    check_x1, check_x2 = y_last_two.index.tolist()
                    check_y1, check_y2 = y_last_two['Close'].tolist()
                    previous_date, current_date = y_last_two['Datetime'].tolist()


                    for index, row in last_five_stock_df.iterrows():
                        previous_status, current_status = None, None

                        x1, x2 = row['row1'],row['row2']
                        y1, y2 = row['value1'],row['value2']
                        m = (y2 - y1) / (x2 - x1)
                        c = y1 - m * x1
                        # line_y1 = row['slope'] + x1 + row['intercept']
                        # line_y2 = row['slope'] + x2 + row['intercept']
                        line_y1 = m * check_x1 + c
                        line_y2 = m * check_x2 + c
                        
                        previous_status = 'above' if check_y1 > line_y1 else 'below' if check_y1 < line_y1 else 'on'
                        current_status = 'above' if check_y2 > line_y2 else 'below' if check_y2 < line_y2 else 'on'

                        #three line breakout
                        point1 = [x1,y1]
                        point2 = [check_x2,check_y2]
                        above_count, below_count,above_percentage,\
                                    below_percentage = point_position_relative_to_line(last_stock_df,point1,point2)

                        # two line breakout
                        percent_difference1 = abs(y1 - check_y2) / check_y2 * 100
                        percent_difference2 = abs(y2 - check_y2) / check_y2 * 100
                        is_same_line = percent_difference1<=1 or percent_difference2<=1

                        line_break_or_not = False
                        if(number_of_line == "three"):
                            line_break_or_not = above_percentage if row['buyORsell'] == 'High' else below_percentage
                        else:
                            line_break_or_not = percent_difference1<=1 or percent_difference2<=1
                        if(previous_status != current_status and line_break_or_not):

                            logging.info(f"{stock_name} - stock breakout found...")

                            if 'value3' in row:
                                output_columns =['stockname','date1','value1','date2','value2','date3','value3','buyORsell']
                            else:
                                output_columns = ['stockname','date1','value1','date2','value2','buyORsell']

                            new_alert = row[output_columns].to_frame().T
                            new_alert.insert(1, 'Alert_date', current_date)
                            new_alert.insert(2, 'Time_Frame', time_frame)
                            break_out_stocks = pd.concat([break_out_stocks, new_alert], ignore_index=True)
                            
                    # logging.info(f"{stock_name} - breakout verification Ended...")
                except Exception as e:
                    logging.info(f"{stock_name} Error : {e}")
                    traceback_msg = traceback.format_exc()
                    logging.info(f"Error : {traceback_msg}")

    return break_out_stocks

def append_to_excel(df_list, excel_file='chartink_data.xlsx'):
    current_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    if os.path.exists(excel_file):
        sheets_dict = pd.read_excel(excel_file, engine="openpyxl", sheet_name=None)
    else:
        sheets_dict = {}
    
    if not df_list:
        return
    
    for key in df_list:
        if key not in sheets_dict:
            sheets_dict[key] = pd.DataFrame()
        # df_list[key]['timeframe'] = key
        df_list[key]['Date Time'] = current_time
        sheets_dict[key] = pd.concat([sheets_dict[key],df_list[key]])

    with pd.ExcelWriter(excel_file) as writer:
        for sheet_name,df in sheets_dict.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

if __name__ =="__main__":
    time_frames = ["hour","day","week","month","quarter"]
    if(len(sys.argv) >= 2):
        time_frames = (sys.argv[1]).split(',')

    os.makedirs(f"log/", exist_ok=True)
    logging.basicConfig(filename=f'log/pdf_report_generator_{",".join(time_frames)}.log',level=logging.INFO, format='%(asctime)s -%(levelname)s - %(message)s')
    logging.info(f"Started...")

    new_breakout_alert = {}
    time_frames_for_yfinance = {
        "hour": "60m",
        "day": "1d",
        "week": "1wk",
        "month": "1mo",
        "quarter": "3mo"
    }
    is_breakout = False
    pdf = FPDF(unit='mm', format=(297, 297))
    pdf.add_page()
    pdf.set_font('Arial', 'B', 16)

    logging.info(f"Three line alert started...")
    three_line_alerts_file_name = "excel/{yfinance_time_frame}/three_line_alerts_{yfinance_time_frame}.xlsx"
    three_breakout_stocks = stock_break_out_finder(time_frames,three_line_alerts_file_name,"three")
    if(not three_breakout_stocks.empty):
        is_breakout = True
        output_df_to_pdf("Three Line Alert",pdf,three_breakout_stocks)
        new_breakout_alert["three_line"] = three_breakout_stocks
    logging.info(f"Three line alert Ended...")

    logging.info(f"Two line alert started...")
    two_line_alerts_file_name = "excel/{yfinance_time_frame}/two_line_alerts_{yfinance_time_frame}.xlsx"
    two_breakout_stocks = stock_break_out_finder(time_frames,two_line_alerts_file_name,"two")
    if(not two_breakout_stocks.empty):
        is_breakout = True
        output_df_to_pdf("Two Line Alert",pdf,two_breakout_stocks)
        new_breakout_alert["two_line"] = two_breakout_stocks
    logging.info(f"Two line alert Ended...")

    logging.info(f"PH PL alert started...")
    ph_pl_alerts_file_name = "excel/{yfinance_time_frame}/ph_pl_data_{yfinance_time_frame}.xlsx"
    ph_pl_breakout_stocks = ph_pl_data_breakout(time_frames,ph_pl_alerts_file_name)
    if(not ph_pl_breakout_stocks.empty):
        is_breakout = True
        output_df_to_pdf("PH PL breakout alert",pdf,ph_pl_breakout_stocks)
        new_breakout_alert["ph_pl_breakout_line"] = ph_pl_breakout_stocks
    logging.info(f"PH PL alert Ended...")

    os.makedirs(f"excel/breakout/", exist_ok=True)
    append_to_excel(new_breakout_alert, excel_file='excel/breakout/line_breakout.xlsx')

    current_time = datetime.datetime.now()
    date_time = current_time.strftime("%Y%m%d.%H%M%S")
    os.makedirs(f"pdf_report/breakout_stocks/", exist_ok=True)
    if(is_breakout):
        pdf_name = f'pdf_report/breakout_stocks/breakout_stocks_{date_time}.pdf'
        pdf.output(pdf_name, 'F')
        telegram_message_send.send_message_with_documents( #message="Breakout Stocks",
                                                          document_paths=[pdf_name],
                                                          captions=[f"breakout Stocks {date_time}"])
    print(three_breakout_stocks)
