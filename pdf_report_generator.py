import yfinance as yf
import pandas as pd
import sys
import logging
import os
import datetime
from fpdf import FPDF

from functions import output_df_to_pdf
import get_candle_data 
import telegram_message_send

def stock_break_out_finder(time_frames,breakout_file_name):
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
            stock_all_df = get_candle_data.get_candle_data_from_yfinance(tickers=stock_names, period='max', interval=yfinance_time_frame)
            last_five_stock_df = stock_df.groupby('stockname').tail(5).sort_values(by='alert_date')
            for stock_name in stock_names:
                if stock_name not in stock_all_df:
                    continue
                # file_name = f"stock_historical_data/{yfinance_time_frame}/{stock_name}.xlsx"
                # isExist = os.path.exists(file_name)
                # if(not isExist):
                #     continue
                
                x = last_five_stock_df[last_five_stock_df['stockname'] == stock_name].tail(1)
                y = stock_all_df[stock_name]
                # y = pd.read_excel(file_name)

                x['Timestamp'] = pd.to_datetime(x['alert_date'], format="%Y-%m-%d %H:%M:%S").apply(lambda z: int(z.timestamp()))
                y['Timestamp'] = pd.to_datetime(y['Datetime'], format="%d-%m-%Y %H:%M:%S").apply(lambda z: int(z.timestamp()))
                
                target_row = y[y['Timestamp'] == x['Timestamp'].iloc[-1]].index[0]
                new_index_value = x['rowNumber'].iloc[-1]

                new_index = list(range(new_index_value - target_row, new_index_value)) + \
                            [new_index_value] + \
                            list(range(new_index_value + 1, new_index_value + len(y) - target_row))
                y.index = new_index
                y = y[-2:]
                x1, x2 = y.index.tolist()
                y1, y2 = y['Close'].tolist()
                previous_date, current_date = y['Datetime'].tolist()

                for index, row in last_five_stock_df.iterrows():
                    previous_status, current_status = None, None

                    line_y1 = row['slope'] + x1 + row['intercept']
                    line_y2 = row['slope'] + x2 + row['intercept']
                    
                    previous_status = 'above' if y1 > line_y1 else 'below' if y1 < line_y1 else 'on'
                    current_status = 'above' if y2 > line_y2 else 'below' if y2 < line_y2 else 'on'

                    if(previous_status != current_status):
                        if 'value3' in row:
                            output_columns =['stockname','date1','value1','date2','value2','date3','value3','buyORsell']
                        else:
                            output_columns = ['stockname','date1','value1','date2','value2','buyORsell']
                        new_alert = row[output_columns].to_frame().T
                        new_alert.insert(1, 'Alert_date', current_date)
                        new_alert.insert(2, 'Time_Frame', time_frame)
                        break_out_stocks = pd.concat([break_out_stocks, new_alert], ignore_index=True)
    return break_out_stocks

if __name__ =="__main__":
    time_frames = ["day","week","month","quarter"]
    if(len(sys.argv) >= 2):
        time_frames = (sys.argv[1]).split(',')

    logging.basicConfig(filename=f'logfile_pdf_file{",".join(time_frames)}.log',level=logging.INFO, format='%(asctime)s -%(levelname)s - %(message)s')
    logging.info(f"Started...")

    time_frames_for_yfinance = {
        "day": "1d",
        "week": "1wk",
        "month": "1mo",
        "quarter": "3mo"
    }

    pdf = FPDF(unit='mm', format=(270, 297))
    pdf.add_page()
    pdf.set_font('Arial', 'B', 16)

    three_line_alerts_file_name = "excel/{yfinance_time_frame}/three_line_alerts_{yfinance_time_frame}.xlsx"
    three_breakout_stocks = stock_break_out_finder(time_frames,three_line_alerts_file_name)
    output_df_to_pdf("Three Line Alert",pdf,three_breakout_stocks)

    two_line_alerts_file_name = "excel/{yfinance_time_frame}/two_line_alerts_{yfinance_time_frame}.xlsx"
    two_breakout_stocks = stock_break_out_finder(time_frames,two_line_alerts_file_name)
    output_df_to_pdf("Ttwo Line Alert",pdf,two_breakout_stocks)

    current_time = datetime.datetime.now()
    date_time = current_time.strftime("%Y%m%d.%H%M%S")
    os.makedirs(f"pdf_report/breakout_stocks/", exist_ok=True)
    pdf_name = f'pdf_report/breakout_stocks_{date_time}.pdf'
    pdf.output(pdf_name, 'F')
    print(three_breakout_stocks)
    telegram_message_send.send_message_with_documents(document_paths=[pdf_name],captions=[f"breakout Stocks {date_time}"])

