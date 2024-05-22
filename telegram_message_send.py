import requests
from datetime import datetime
import pytz
import logging
import os

def send_message_with_documents(message="", document_paths=[],captions=[]):
    bot_token = '6511501073:AAHbWvFY_dKcUQfKNGFODOeYK8PEUJ4vXPI'
    chat_id = -4202551900
    # bot_token = os.environ["BOT_TOKEN"]
    # chat_id = os.environ["CHAT_ID"]

    if message:
        url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
        params = {
            'chat_id': chat_id,
            'text': message
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            logging.info(f'Message sent successfully! - {message}')
        else:
            logging.info(f'Failed to send message: - { response.text }')

    document_url = f'https://api.telegram.org/bot{bot_token}/sendDocument'
    for path,caption in zip(document_paths,captions):
        document_params = {
            'chat_id': chat_id,
            'caption' : caption
        }
        document_file = {'document': open(path, 'rb')}
        document_response = requests.post(document_url, params=document_params, files=document_file)
        print(document_response.status_code)
        if document_response.status_code == 200:
            logging.info(f'Message sent successfully! - {message}')
        else:
            logging.info(f'Failed to send message: - { document_response.text }')

if __name__=="__main__":
    message = ''
    document_paths = ['pdf_report/chartink/3_V_version_20240522.pdf']
    send_message_with_documents(message=message, document_paths=document_paths,captions=["Line pattern","Line pattern"])