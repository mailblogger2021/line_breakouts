import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from fpdf import FPDF
import sys
import threading
import logging
import os
import json
import traceback
import datetime
from telegram_message_send import send_message_with_documents

def chartink_to_pdf(session,title, pdf,chartink_url):
    r = session.post('https://chartink.com/screener/process', data={'scan_clause': chartink_url}).json()
    df = pd.DataFrame(r['data'])
    if df.empty:
        return df
    
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

    return df
    # if not df.empty:
    # return df['nsecode'].unique().tolist()

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

if __name__=="__main__":

    os.makedirs(f"pdf_report/chartink/", exist_ok=True)
    current_time = datetime.datetime.now()
    date_time = current_time.strftime("%Y%m%d")

    pdf = FPDF(unit='mm', format=(250, 297))
    pdf.add_page()
    pdf.set_font('Arial', 'B', 16)
    message,document_paths,captions = [],[],[]

    new_chartink_alert = {}
    base_code_list = []
    title_list = []
    sheet_names = []

    base_code = "( {46553} ( latest close / 1 day ago max ( 21 , 1 day ago high ) <= 0.9 and 1 day ago close / 2 days ago max ( 21 , 2 days ago high ) > 0.9 and 1 day ago close / 1 day ago max ( 5 , 1 day ago high ) <= 0.95 ) ) "
    base_code_list.append(base_code)

    title = "Day Percentage changes - last 21 days 10 per change - NIFTY 200"
    title_list.append(title)
    sheet_names.append("10per change")

    with requests.Session() as session:
        r = session.get('https://chartink.com/screener/time-pass-48')
        soup = bs(r.content, 'lxml')
        session.headers['X-CSRF-TOKEN'] = soup.select_one('[name=csrf-token]')['content']
        for base_code, title,sheet_name in zip(base_code_list, title_list,sheet_names):
            new_chartink_alert[sheet_name] = chartink_to_pdf(session,title,pdf,base_code)

    file_name = f"pdf_report/chartink/my_screener_{date_time}.pdf"
    pdf.output(file_name, 'F')
    document_paths.append(file_name)
    captions.append("my_screener")


    pdf = FPDF(unit='mm', format=(250, 297))
    pdf.add_page()
    pdf.set_font('Arial', 'B', 16)

    base_code_list = []
    title_list = []
    sheet_names = []

    base_code = "( {cash} ( quarterly volume >= 1 quarter ago volume * 1.25 and 1 quarter ago volume >= 10000000 and quarterly volume >= 10000000 and ( quarterly max ( 4 , 1 quarter ago close ) ) < quarterly close and ( 1 quarter ago  max ( 4 , 1 quarter ago close )) >= 1 quarter ago  close ) ) "
    base_code_list.append(base_code)

    title = "Quarterly breakout"
    title_list.append(title)
    sheet_names.append("Quarterly breakout")

    base_code = "( {cash} ( market cap > 1000 and latest close > 50 and latest sma ( latest volume , 20 ) > 500000 and 1 day ago sma ( 1 day ago high / 1 day ago low , 40 ) < 1.3 and ( {cash} ( 1 day ago max ( 10 , latest high ) < 11 days ago max ( 15 , latest high ) and 26 days ago max ( 15 , latest high ) < 41 days ago max ( 19 , latest high ) and 1 day ago min ( 10 , latest low ) > 11 days ago min ( 15 , latest low ) and 26 days ago min ( 15 , latest low ) > 41 days ago min ( 19 , latest low ) ) ) and ( {cash} ( 1 day ago high < 2 days ago max ( 58 , latest high ) and 1 day ago low > 2 days ago min ( 40 , latest low ) and ( 1 day ago max ( 10 , latest high ) - 1 day ago min ( 10 , 1 day ago low ) ) < ( 26 days ago max ( 15 , latest high ) - 26 days ago min ( 15 , latest low ) ) and ( 26 days ago max ( 15 , latest high ) - 26 days ago min ( 15 , 1 day ago low ) ) < ( 41 days ago max ( 19 , latest high ) - 41 days ago min ( 19 , latest low ) ) and latest volume > latest ema ( latest volume , 20 ) * 2 and ( {cash} ( latest close > 1 day ago max ( 10 , latest high ) ) ) ) ) ) ) "
    base_code_list.append(base_code)

    title = "3 V 0.3"
    title_list.append(title)
    sheet_names.append("3 V 0.3")


    base_code = "( {cash} ( market cap > 1000 and latest close > 50 and latest sma ( latest volume , 20 ) > 500000 and 1 day ago sma ( 1 day ago high / 1 day ago low , 40 ) < 1.3 and ( {cash} ( ( {cash} ( 1 day ago high < 2 days ago max ( 58 , latest high ) and 1 day ago low > 2 days ago min ( 58 , latest low ) and 1 day ago max ( 10 , latest high ) < 11 days ago max ( 15 , latest high ) and 26 days ago max ( 15 , latest high ) < 41 days ago max ( 19 , latest high ) and 1 day ago min ( 10 , latest low ) > 11 days ago min ( 15 , latest low ) and 26 days ago min ( 15 , latest low ) > 41 days ago min ( 19 , latest low ) and 1 day ago countstreak( 59, 1 where latest high < 2 days ago max ( 58 , latest high ) ) > 20 and 1 day ago countstreak( 59, 1 where latest low > 2 days ago min ( 58 , latest low ) ) > 20 and ( {cash} ( latest close > 1 day ago max ( 59 , latest high ) ) ) ) ) ) ) ) ) "
    base_code_list.append(base_code)

    title = "DND 3 V 0.3"
    title_list.append(title)
    sheet_names.append("DND 3 V 0.3")

    base_code = '( {cash} ( market cap > 1000 and latest close > 50 and latest sma ( latest volume , 20 ) > 500000 and ( {cash} ( latest close > latest open and 1 day ago min ( 3 , latest open - latest close ) > 0 and ( {cash} ( ( {cash} ( latest low > 1 day ago low and latest close > ( 1 day ago "greatest(   open,  close  )" + 1 day ago "least(   open,  close  )" ) / 2 ) ) or ( {cash} ( latest low < 1 day ago low and ( {cash} ( latest close > 1 day ago close or latest close > ( 1 day ago "greatest(   open,  close  )" + 1 day ago "least(   open,  close  )" ) / 2 ) ) ) ) or ( {cash} ( latest low < 1 day ago low and ( latest "greatest(   open,  close  )" / latest "least(   open,  close  )" ) > 1.007 and ( latest high - latest "greatest(   open,  close  )" ) <= latest ""greatest(   open,  close  )" -  "least(   open,  close  )"" and ( latest high - latest "greatest(   open,  close  )" ) <= ( latest "least(   open,  close  )" - latest low ) / 3 and ( latest "least(   open,  close  )" - latest low ) > latest ""greatest(   open,  close  )" -  "least(   open,  close  )"" * 3 ) ) ) ) ) ) and 1 day ago low < 2 days ago min ( 5 , latest low ) and 2 days ago min ( 5 , latest low ) < 7 days ago min ( 20 , 2 days ago low ) and 1 day ago min ( 5 , latest rsi  ( 14 ) ) < 34 ) ) '
    base_code_list.append(base_code)

    title = "3 V 0.4"
    title_list.append(title)
    sheet_names.append("3 V 0.4")


    # new_chartink_alert = {}
    with requests.Session() as session:
        r = session.get('https://chartink.com/screener/time-pass-48')
        soup = bs(r.content, 'lxml')
        session.headers['X-CSRF-TOKEN'] = soup.select_one('[name=csrf-token]')['content']
        for base_code, title,sheet_name in zip(base_code_list, title_list,sheet_names):
            new_chartink_alert[sheet_name] = chartink_to_pdf(session,title,pdf,base_code)

    file_name = f"pdf_report/chartink/3_V_version_{date_time}.pdf"
    pdf.output(file_name, 'F')
    document_paths.append(file_name)
    captions.append("3 V 0.3")

    os.makedirs(f"excel/breakout/", exist_ok=True)
    append_to_excel(new_chartink_alert, excel_file='excel/breakout/chartink_screener.xlsx')

    send_message_with_documents(message=[], document_paths=document_paths,captions=captions)
    print("Done")
