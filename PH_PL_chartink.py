import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from fpdf import FPDF
import sys
import two_line_pattern_detect_class as pattern_detect_class
import threading
import logging
import os
import json
import traceback
import datetime

now = datetime.datetime.now()
print("now =", now)
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
print("date and time =", dt_string)

def chartink_to_pdf(session,title, pdf,chartink_url):
    r = session.post('https://chartink.com/screener/process', data={'scan_clause': chartink_url}).json()
    df = pd.DataFrame(r['data'])
    if df.empty:
        return []
    
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(40, 20, title, ln=True)
    pdf.ln(2)
    
    table_cell_height = 6
    
    cols = df.columns
    content = df.values.tolist()
    
    max_widths = [pdf.get_string_width(col) for col in cols]
    for row in content:
        for i, cell in enumerate(row):
            width = pdf.get_string_width(str(cell))*50//100
            if width > max_widths[i]:
                max_widths[i] = width
    
    pdf.set_font('Arial', '', 6)
    cols = df.columns
    for i, col in enumerate(cols):
        pdf.cell(max_widths[i], table_cell_height, col, align='C', border=1)
    pdf.ln(table_cell_height)

    for row in content:
        for i, cell in enumerate(row):
            # Set cell width based on maximum content width in the column
            pdf.cell(max_widths[i], table_cell_height, str(cell), align='C', border=1)
        pdf.ln(table_cell_height)
    pdf.ln(10)
    # if not df.empty:
    return df['nsecode'].unique().tolist()
    # else:
        # return []

def generate_chartink_code(time_frame_list=[],base_code_list=[],title_list=[],file_name='chartink_data_pdf'):
    
    pdf = FPDF(unit='mm', format=(250, 297))
    pdf.add_page()
    pdf.set_font('Arial', 'B', 16)

    ph_pl_list = {}
    with requests.Session() as session:
        r = session.get('https://chartink.com/screener/time-pass-48')
        soup = bs(r.content, 'lxml')
        session.headers['X-CSRF-TOKEN'] = soup.select_one('[name=csrf-token]')['content']
        for time_frame,base_code, title in zip(time_frame_list,base_code_list, title_list):
            ph_pl_list[time_frame] = chartink_to_pdf(session,title,pdf,base_code)

    pdf.output(f'{file_name}.pdf', 'F')
    return ph_pl_list

if __name__ =="__main__":
    
    time_frames = ["hour","day","week","month","quarter"]
    if(len(sys.argv) >= 2):
        time_frames = (sys.argv[1]).split(',')

    os.makedirs(f"log/", exist_ok=True)
    logging.basicConfig(filename=f'log/logfile_ph_pl_{",".join(time_frames)}.log',level=logging.INFO, format='%(asctime)s -%(levelname)s - %(message)s')
    logging.info(f"Started...")

    time_frames_for_yfinance = {
                "hour":"60m",
                "day" : "1d",
                "week" : "1wk",
                "month" : "1mo",
                "quarter" : "3mo"
            }
    # day_ph_pl = "( {33489} ( ( {33489} ( 10 days ago high > latest max ( 10 , 11 days ago high ) and 10 days ago high >= latest max ( 10 , latest high ) ) ) or ( {33489} ( 10 days ago low < latest min ( 10 , 11 days ago low ) and 10 days ago low <= latest min ( 10 , latest low ) ) ) ) )"
    ph_pl_list = {
        "hour" : "( {57960} ( [-10] 1 hour low <= [0] 1 hour min ( 21 , [0] 1 hour low ) or [-10] 1 hour high >= [0] 1 hour max ( 21 , [0] 1 hour high ) ) ) ",
        "day" : "( {33489} ( 10 days ago high >= latest max ( 21 , latest high ) or 10 days ago low <= latest min ( 21 , latest low ) ) ) ",
        "week" : "( {33489} ( 5 weeks ago high >= weekly max ( 11 , weekly high ) or 5 weeks ago low <= weekly min ( 11 , weekly low ) ) ) ",
        "month" : "( {33489} ( 5 months ago high >= monthly max ( 11 , monthly high ) or 5 months ago low <= monthly min ( 11 , monthly low ) ) ) ",
        "quarter" : "( {33489} ( 5 quarters ago high >= quarterly max ( 11 , quarterly high ) or 5 quarters ago low <= quarterly min ( 11 , quarterly low ) ) ) " 
    }
    base_code_list,title_list,time_frame_list = [],[],[]
    # time_frames = time_frames[:1]
    for time_frame in time_frames:
        base_code = ph_pl_list[time_frame]
        time_frame_list.append(time_frame)
        base_code_list.append(base_code)
        title_list.append(time_frame+" time Frame new PH PL ")
    current_time = datetime.datetime.now()
    date_time = current_time.strftime("%Y%m%d.%H%M%S")
    pdf_name = f"pdf_report/PH_PL/PH_PL_chartink_{date_time}"
    os.makedirs(f"pdf_report/PH_PL/", exist_ok=True)
    ph_pl_list = generate_chartink_code(time_frame_list,base_code_list,title_list,pdf_name)
    threads = []
    for time_frame in ph_pl_list:
        print(time_frame)
        stock_lists = ph_pl_list[time_frame]
        stock_lists = [stock+".NS" for stock in stock_lists]

        yfinance_time_frame = time_frames_for_yfinance[time_frame]

        input_json_file = "input.json"
        with open(input_json_file, "r") as file:
            input_data = json.load(file)
        
        window,percentage,pivot_line_count,two_line_count = input_data[yfinance_time_frame]["window"],input_data[yfinance_time_frame]["percentage"], \
                                                        input_data[yfinance_time_frame]["pivot_line_count"],input_data[yfinance_time_frame]["two_line_count"]
        try:
            obj = pattern_detect_class.pattern_detecter(yfinance_time_frame,window,percentage,pivot_line_count,two_line_count)
            # obj.data_store[yfinance_time_frame] = list(set(obj.data_store[yfinance_time_frame]) - set(stock_lists))
            obj.generate_url_yfinance(stock_lists)
            obj.save_excel_file()

        except Exception as e:
            logging.info(f"{time_frame} - Error in PH PL main function: {e}")
            traceback_msg = traceback.format_exc()
            logging.info(f"{time_frame} - Error : {traceback_msg}")

            print(f"{time_frame} - Error in PH PL main function: {e}")
            print(f"{time_frame} - Error : {traceback_msg}")
    print(ph_pl_list)
