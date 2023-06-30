import io
import os
import re
import glob
import json
import xml.etree.ElementTree as ET
import zipfile
import requests
from datetime import datetime

import pandas as pd

from typing import Dict, Any
from utils import make_api_call

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'


def get_corp_code_list(api_key: str) -> pd.DataFrame:
    url = 'https://opendart.fss.or.kr/api/corpCode.xml'
    params = { 'crtfc_key': api_key, }

    r = make_api_call(url, params)
    try:
        tree = ET.XML(r.content)
        status = tree.find('status').text
        message = tree.find('message').text
        if status != '000':
            raise ValueError({'status': status, 'message': message})
    except ET.ParseError as e:
        pass

    zf = zipfile.ZipFile(io.BytesIO(r.content))
    xml_data = zf.read('CORPCODE.xml')

    # XML to DataFrame
    tree = ET.XML(xml_data)
    all_records = []

    element = tree.findall('list')
    for _, child in enumerate(element):
        record = {}
        for _, subchild in enumerate(child):
            record[subchild.tag] = subchild.text
        all_records.append(record)
    return pd.DataFrame(all_records)

def company_info(api_key: str, corp_code: str) -> Dict:
    url = 'https://opendart.fss.or.kr/api/company.json'
    params = {'crtfc_key':api_key, 'corp_code': corp_code}

    r = make_api_call(url, params)
    try:
        tree = ET.XML(r.content)
        status = tree.find('status').text
        message = tree.find('message').text
        if status != '000':
            raise ValueError({'status': status, 'message': message})
    except ET.ParseError as e:
        jo = r.json()
        if jo['status'] != '000':
            print(ValueError(r.text))

    jo = r.json()

    results = {
            'company_name': jo['corp_name'],
            'company_name_eng': jo['corp_name_eng'],
            'stock_code': jo['stock_code'],
            'ceo_name': jo['ceo_nm'],
            'address': jo['adres'],
            'induty_code': jo['induty_code'],
            'establish_date': jo['est_dt'],
        }
    
    return results
        


def corp_code_list(api_key: str) -> pd.DataFrame:
    docs_cache_dir = 'docs_cache'
    if not os.path.exists(docs_cache_dir):
        os.makedirs(docs_cache_dir)

    # read and return document if exists
    fn = 'opendartreader_corp_codes_{}.pkl'.format(datetime.today().strftime('%Y%m%d'))
    fn_cache = os.path.join(docs_cache_dir, fn)
    for fn_rm in glob.glob(os.path.join(docs_cache_dir, 'opendartreader_corp_codes_*')):
        if fn_rm == fn_cache:
            continue
        os.remove(fn_rm)
    if not os.path.exists(fn_cache):
        df = get_corp_code_list(api_key)
        df.to_pickle(fn_cache)

    corp_codes = pd.read_pickle(fn_cache)

    return corp_codes


def download_corp_document(params: dict) -> pd.DataFrame:
    url = 'https://opendart.fss.or.kr/api/list.json'

    r = make_api_call(url, params)
    try:
        tree = ET.XML(r.content)
        status = tree.find('status').text
        message = tree.find('message').text
        if status != '000':
            raise ValueError({'status': status, 'message': message})
    except ET.ParseError as e:
        jo = r.json()
        if jo['status'] != '000':
            print(ValueError(r.text))

    jo = r.json()
    if 'list' not in jo:
        return pd.DataFrame()
    df = pd.DataFrame(jo['list'])

    for page in range(2, jo['total_page']+1):
        params['page_no'] = page
        r = make_api_call(url, params)
        jo = r.json()
        df = pd.concat([df, pd.DataFrame(jo['list'])])
        df = df.reset_index(drop=True)

    return df


def sub_docs(rcp_no: str) -> pd.DataFrame:
    if rcp_no.isdecimal():
        r = make_api_call(f'http://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}')
    elif rcp_no.startswith('http'):
        r = make_api_call(rcp_no)
    else:
        raise ValueError('invalid `rcp_no`(or url)')
    
    ## 하위 문서 URL 추출
    multi_page_re = (
        "\s+node[12]\['text'\][ =]+\"(.*?)\"\;" 
        "\s+node[12]\['id'\][ =]+\"(\d+)\";"
        "\s+node[12]\['rcpNo'\][ =]+\"(\d+)\";"
        "\s+node[12]\['dcmNo'\][ =]+\"(\d+)\";"
        "\s+node[12]\['eleId'\][ =]+\"(\d+)\";"
        "\s+node[12]\['offset'\][ =]+\"(\d+)\";"
        "\s+node[12]\['length'\][ =]+\"(\d+)\";"
        "\s+node[12]\['dtd'\][ =]+\"(.*?)\";"
        "\s+node[12]\['tocNo'\][ =]+\"(\d+)\";"
    )

    matches = re.findall(multi_page_re, r.text)
    if len(matches) > 0:
        row_list = []
        for m in matches:
            doc_id = m[1]
            doc_title = m[0]
            params = f'rcpNo={m[2]}&dcmNo={m[3]}&eleId={m[4]}&offset={m[5]}&length={m[6]}&dtd={m[7]}'
            doc_url = f'http://dart.fss.or.kr/report/viewer.do?{params}'
            row_list.append([doc_title, doc_url])
        df = pd.DataFrame(row_list, columns=['title', 'url'])
        return df[['title', 'url']]
    else:
        raise Exception(f'{rcp_no} 하위 페이지를 포함하고 있지 않습니다.')      





