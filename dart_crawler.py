import os
import re
import math
import json
import time
import requests
from datetime import datetime
from tqdm import tqdm

from typing import List, Optional

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

import dart_api
import utils

from utils import make_api_call

DATASET_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'datasets')
if not os.path.exists(DATASET_DIR):
	os.mkdir(DATASET_DIR)
        
with open("config.json") as fin:
    config = json.load(fin)['dart_crawler']

api_key = config['api_key']
corp_codes = dart_api.corp_code_list(api_key)
  

def find_corp_code(corp: str) -> Optional[str]:
    if not corp.isdigit():
        df = corp_codes[corp_codes['corp_name'] == corp]
    elif corp.isdigit() and len(corp) == 6:
        df = corp_codes[corp_codes['stock_code'] == corp]
    else:
        df = corp_codes[corp_codes['corp_code'] == corp]
    return None if df.empty else df.iloc[0]['corp_code']
        

def main():

    raw_filings_folder = os.path.join(DATASET_DIR, config['raw_filings_folder'])
    indices_folder = os.path.join(DATASET_DIR, config['indices_folder'])
    filings_metadata_filepath = os.path.join(DATASET_DIR, config['filings_metadata_file'])
    

    if len(config['filing_types']) == 0:
        print("Please provide at least one filing type")
        exit()

    if len(api_key) == 0:
        print("Please get api key from dart")
        exit()

    if not os.path.isdir(indices_folder):
        os.mkdir(indices_folder)
    if not os.path.isdir(raw_filings_folder):
        os.mkdir(raw_filings_folder)

    if not os.path.isfile(os.path.join(DATASET_DIR, 'companies_info.json')):
        with open(os.path.join(DATASET_DIR, 'companies_info.json'), 'w') as f:
            json.dump(obj={}, fp=f)

    download_indices(
		start_year=config['start_year'],
		end_year=config['end_year'],
		quarters=config['quarters'],
		indices_folder=indices_folder,
        filing_types=config['filing_types'],
		api_key=api_key,
	)
    
    csv_filenames = []
    for year in range(config['start_year'], config['end_year'] + 1):
        for quarter in config['quarters']:
            filepath = os.path.join(indices_folder, f'{year}_QTR{quarter}.csv')

            if os.path.isfile(filepath):
                csv_filenames.append(filepath)

    df = get_specific_indices(
		csv_filenames=csv_filenames,
		filing_types=config['filing_types'],
		cik_tickers=config['cik_tickers'],
	)
    old_df = []
    if os.path.exists(filings_metadata_filepath):
        old_df = []
        series_to_download = []
        print(f'\nReading filings metadata...\n')

        for _, series in pd.read_csv(filings_metadata_filepath, dtype=str).iterrows():
            #filename 정해야 함
            if os.path.exists(os.path.join(raw_filings_folder, series['filename'])):
                old_df.append((series.to_frame()).T)

        if len(old_df) == 1:
            old_df = old_df[0]
        elif len(old_df) > 1:
            old_df = pd.concat(old_df)

        #column 수정해야 함
        for _, series in tqdm(df.iterrows(), total=len(df), ncols=100):
            if len(old_df) == 0 or len(old_df[old_df['rcept_no']== series['rcept_no']]) == 0:
                series_to_download.append((series.to_frame()).T)

        if len(series_to_download) == 0:
            print(f'\nThere are no more filings to download for the given years, quarters and companies')
            exit()

        df = pd.concat(series_to_download) if (len(series_to_download) > 1) else series_to_download[0]

    list_of_series = []
    for i in range(len(df)):
        list_of_series.append(df.iloc[i])

    print(f"\nDownloading {len(df)} filings...\n")

    final_series = []
    for series in tqdm(list_of_series, ncols=100):
        series = crawl(
			series=series,
			filing_types=config['filing_types'],
            raw_filings_folder=raw_filings_folder,
            api_key=api_key,
            user_agent=config['user_agent']
        )
        if series is not None:
            final_series.extend([series.iloc[i].to_frame().T for i in range(len(series))])
            final_df = pd.concat(final_series) if (len(final_series) > 1) else final_series[0]
            if len(old_df) > 0:
                final_df = pd.concat([old_df, final_df])
            final_df.to_csv(filings_metadata_filepath, index=False, header=True)

    if len(final_series) < len(list_of_series):
        print(f"\nDownloaded {len(final_series)} / {len(list_of_series)} filings.")
        print(f'Rerun the script to retry downloading the failed filings.')


def crawl(
		series: pd.Series,
        filing_types: str,
        raw_filings_folder: str,
        user_agent: str,
        api_key: str,
) -> pd.DataFrame:
    rcp_no = series['rcept_no']

    df = dart_api.sub_docs(rcp_no)
    time.sleep(1)

    for col in series.to_frame().T:
        df[col] = np.vstack([series[col]]*len(df))

    df['filing_types'] = filing_types
    
    with open(os.path.join(DATASET_DIR, 'companies_info.json'), encoding="utf-8") as f:
        company_info_dict = json.load(fp=f)
    corp_code = series['corp_code']
    if corp_code not in company_info_dict:
        c_info = dart_api.company_info(api_key, corp_code)
        time.sleep(1)
        company_info_dict[corp_code] = c_info

        with open(os.path.join(DATASET_DIR, 'companies_info.json'), 'w') as f:
            json.dump(obj=company_info_dict, fp=f, indent=4, ensure_ascii=False)

    filename = f'{series["stock_code"]}_{filing_types}_{df["year"].unique()[0]}_{series["rcept_no"]}_{series["rcept_dt"]}.html'
    df['filename'] = filename
    with open(os.path.join(raw_filings_folder, filename), 'w') as of:
        for _, row in df.iterrows():
            of.write(f"<!-- File: {os.path.basename(row['title'])} -->\n")
            r = make_api_call(row['url'])
            time.sleep(2)
            if r.status_code==200:
                soup = BeautifulSoup(r.text, 'lxml')
                of.write(str(soup))
            else:
                print(f"Crawling Error...{series['stock_code']}")
                return None
    return df

        
def download_indices(
        start_year: int,
        end_year: int,
        quarters: List,
        indices_folder: str,
        filing_types: str,
        api_key: str
) -> None:
    for quarter in quarters:
        if quarter not in [1, 2, 3, 4]:
            raise Exception(f'Invalid quarter "{quarter}"')
        

    first_iteration = True
    while True:
        failed_indices = []
        for year in range(start_year, end_year + 1):
            for quarter in quarters:
                bgn_de = utils.get_quarter_start_date(year, quarter)
                nxt_quarter = (quarter % 4) + 1
                nxt_year = year + 1 if nxt_quarter < quarter else year
                if nxt_year == datetime.now().year and nxt_quarter > math.ceil(datetime.now().month / 3):
                    break
                end_de = utils.get_quarter_start_date(nxt_year, nxt_quarter)

                index_filename = f'{year}_QTR{quarter}.csv'
                if os.path.exists(os.path.join(indices_folder, index_filename)):
                    if first_iteration:
                        print(f"Skipping {index_filename}")
                        continue

                params = {
                    'crtfc_key': api_key,
                    'bgn_de': bgn_de,
                    'end_de': end_de,
                    'last_reprt_at': 'Y', # 최종보고서 여부
                    'corp_cls': 'Y',
                    'pblntf_detail_ty': filing_types,
                    'page_no': 1,
                    'page_count': 100,
                }
                try:
                    indices = dart_api.download_corp_document(params)
                    indices['year'] = indices['report_nm'].apply(utils.parsing_date)
                    indices = indices[indices['year'] == int(bgn_de[:4])]
                    indices.to_csv(os.path.join(indices_folder, index_filename))
                except Exception as e:
                    print(e)
                    failed_indices.append(index_filename)
                    continue

        first_iteration = False
        if len(failed_indices) > 0:
            print(f'Could not download the following indices:\n{failed_indices}')
            user_input = input('Retry (Y/N): ')
            if user_input in ['Y', 'y', 'yes']:
                print(f'Retry downloading failed indices')
                print(params)
            else:
                break
        else:
            break


def get_specific_indices(
    csv_filenames: List,
    filing_types: str,
    cik_tickers: List =None,
) -> pd.DataFrame:
    total = pd.DataFrame()
    for csv_filename in csv_filenames:
        tmp = pd.read_csv(csv_filename, index_col=0, dtype=str)
        total = pd.concat([total, tmp], ignore_index=True)
    
    #cik_tickers를 stock_code, corp_name, corp_code 중 어떤 것으로 사용할지는 고려해야함
    #만약 corp_name이나 corp_code인 경우 추가 작업 필요
    total = total[total['stock_code'].isin(cik_tickers)]

    return total


if __name__ == '__main__':
    main()