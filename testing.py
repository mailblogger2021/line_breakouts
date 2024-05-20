import requests
from datetime import datetime
import pytz
import threading
import logging
 
def setMessagefromTelegram(msg):
    bot_token = '6511501073:AAHbWvFY_dKcUQfKNGFODOeYK8PEUJ4vXPI'
    chat_id = -4122105586
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S')
    msg_with_time = f'{msg} : {current_time}'
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    params = {
        'chat_id': chat_id,
        'text': msg_with_time
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        print(f'Message sent successfully! - {msg_with_time}')
    else:
        print('Failed to send message:', response.text)

time_frame = "day"

import logging

# Formatter configuration
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""
    # Handler configuration
    logging.basicConfig(level=level)
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

# Configure basic logging for the root logger
# logging.basicConfig(level=logging.DEBUG)

# First file logger
# logger = setup_logger('first_logger', 'first_logfile.log')
# logger.info('This is just an info message')

# logging.basicConfig(level=logging.DEBUG)
# super_logger = setup_logger('second_logger', 'second_logfile.log',level=logging.DEBUG)
# super_logger.error('This is an error message')

# def another_method():
#     # Using logger defined above also works here
#     logger.info('Inside method')
logging.basicConfig(filename=f'first_logger.log',level=logging.DEBUG, format='%(asctime)s -%(levelname)s - %(message)s')
logging.info(f"Started...")


url = 'https://catfact.ninja/fact'
url_test_new_list = []
def url_test_new(i):
    session = requests.Session()
    session.cookies.clear()
    try:
        response = session.get(url, 
                               proxies={"http": "http://111.233.225.166:1234"},
                               headers={'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'}
                               )
        print(i,end=" ")
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
    logging.info(f"{i} - This is a DEBUG level message.")

for index in range(0,10,10):
    threads = []
    for index_j in range(index,index+10):
        thread = threading.Thread(target=url_test_new, args=(index_j,))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
