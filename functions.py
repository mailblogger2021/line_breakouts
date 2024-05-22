from fpdf import FPDF
import pandas as pd
import datetime
import yaml
import telegram_message_send
import os

pdf = FPDF(unit='mm', format=(250, 297))
pdf.add_page()
pdf.set_font('Arial', 'B', 16)
days = 5
pdf.cell(40, 20, "title", ln=True, align='C')
def output_df_to_pdf(title,pdf, df):
    if df.empty:
        return
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(40, 20, title, ln=True, align='C')
    pdf.ln(2)
    table_cell_width = 25
    table_cell_height = 6
    pdf.set_font('Arial', 'B', 6)
    cols = df.columns
    for col in cols:
        pdf.cell(table_cell_width, table_cell_height, col, align='C', border=1)
    pdf.ln(table_cell_height)
    pdf.set_font('Arial', '', 6)
    for row in df.itertuples():
        for col in cols:
            value = str(getattr(row, col))
            pdf.cell(table_cell_width, table_cell_height, value, align='C', border=1)
        pdf.ln(table_cell_height)
    pdf.ln(10)


def pdf_generater(time_frame,days):
    alerts = pd.read_excel(f"three_line_alerts_{time_frame}.xlsx")
    alerts.columns = alerts.columns.astype(str)
    alerts = alerts[["stockname","alert_date","date1","value1","date2","value2","date3","value3","buyORsell"]]
    numeric_cols = alerts.select_dtypes(include='number').columns
    alerts[numeric_cols] = alerts[numeric_cols].round(3)

    current_date = pd.Timestamp.now()
    start_date = current_date - pd.Timedelta(days=days)
    filtered_alerts = alerts[alerts['alert_date'] > start_date]
    filtered_alerts = filtered_alerts.sort_values(by='alert_date')

    output_df_to_pdf("Three Lines alert",pdf, filtered_alerts)

    alerts = pd.read_excel(f"two_line_alerts_{time_frame}.xlsx")
    alerts.columns = alerts.columns.astype(str)
    alerts = alerts[["stockname","alert_date","date1","value1","date2","value2","buyORsell"]]
    numeric_cols = alerts.select_dtypes(include='number').columns
    alerts[numeric_cols] = alerts[numeric_cols].round(3)

    current_date = pd.Timestamp.now()
    start_date = current_date - pd.Timedelta(days=days)
    filtered_alerts = alerts[alerts['alert_date'] > start_date]
    filtered_alerts = filtered_alerts.sort_values(by='alert_date')

    output_df_to_pdf("two Lines alert",pdf, filtered_alerts)

    current_time = datetime.datetime.now()
    date_time = current_time.strftime("%d_%m_%Y_%H_%M_%S")
    os.makedirs(f"pdf_report/{time_frame}", exist_ok=True)
    file_name = f'pdf_report/{time_frame}/Line_pattern_pdf_report_{time_frame}_{date_time}.pdf'
    pdf.output(file_name, 'F')
    
    telegram_message_send.send_message_with_documents(document_paths=[file_name])

def yaml_file_edit(minutes,file_name):
    current_time = datetime.datetime.now()
    cron_time = current_time + datetime.timedelta(minutes=minutes)

    cron_value = f"{cron_time.minute} {cron_time.hour} {cron_time.day} {cron_time.month} *"

    file_name = "day"
    yaml_file_path = f'.github/workflows/{file_name}.yml'
    with open(yaml_file_path, 'r') as file:
        yaml_data = yaml.safe_load(file)

    yaml_data['on']['schedule'][0]['cron'] = cron_value 

    with open(yaml_file_path, 'w') as file:
        yaml.dump(yaml_data, file)

if __name__=="__main__":
    time_frame = "60minute"
    # pdf_generater(time_frame,2)
    # yaml_file_editing.yaml_file_edit(5,time_frame)