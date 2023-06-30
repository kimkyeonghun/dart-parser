import re
from datetime import datetime
from pykrx import stock

import time
from functools import wraps
import requests

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"


def get_quarter_start_date(year, quarter):
    start_month = (quarter - 1) * 3 + 1
    start_day = 1
    start_date = datetime(year, start_month, start_day)
    formatted_date = start_date.strftime("%Y%m%d")
    return formatted_date

def parsing_date(string):
    pattern = r"\((\d{4})\.(\d{2})\)"
    match = re.search(pattern, string)
    try:
        year = int(match.group(1))
    except:
        #invalid
        year = 0000
    return year+1


def tps_limited(tps_limit):
    def decorator(func):
        last_call_time = None
        call_count = 0

        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal last_call_time, call_count

            if last_call_time and time.time() - last_call_time < 1:
                time.sleep(1)

            response = func(*args, **kwargs)

            if response.status_code == 200:
                call_count += 1
                if call_count >= tps_limit:
                    last_call_time = time.time()
                    call_count = 0
                else:
                    last_call_time = None

            while last_call_time and time.time() - last_call_time < 1:
                time.sleep(0.1)

            return response

        return wrapper

    return decorator

@tps_limited(tps_limit=1)
def make_api_call(url, params=None):
    return requests.get(url, params= params, headers={'User-agent': USER_AGENT})

def check_roman_numerals(string):
    pattern = r'\b(I{1,3}|IV|V|IX|X{1,3}|VI{0,3}|XI{0,3}|XII{0,3})\b'
    if re.search(pattern, string):
        return True
    else:
        return False

