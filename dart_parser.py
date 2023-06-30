import re
import os
import json
from html.parser import HTMLParser

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

from pathos.pools import ProcessPool

import roman

from tqdm import tqdm
from typing import List

from utils import check_roman_numerals


DATASET_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'datasets')

delimiter = "<!-- File:"

regex_flags = re.IGNORECASE | re.DOTALL | re.MULTILINE

if not os.path.exists(DATASET_DIR):
    print("??")
    exit()

with open('config.json') as fin:
    config = json.load(fin)['extract_items']

class HtmlStripper(HTMLParser):
    """
    Strips HTML tags
    """

    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return ''.join(self.fed)

    def strip_tags(self, html):
        self.feed(html)
        return self.get_data()
    

class ExtractItems:
    def __init__(
            self,
            remove_tables: bool,
            items_to_extract: List,
            raw_files_folder: str,
            extracted_files_folder: str,
            skip_extracted_filings: bool,
    ):
        self.remove_tables = remove_tables
        self.items_list = [i for i in range(1, 13)]
        self.items_to_extract = items_to_extract if items_to_extract else self.items_list
        self.raw_files_folder = raw_files_folder
        self.extracted_files_folder = extracted_files_folder
        self.skip_extracted_filings = skip_extracted_filings

    @staticmethod
    def remove_multiple_lines(text):
        """
        Replaces consecutive new lines with a single new line
        and consecutive whitespace characters with a single whitespace
        :param text: String containing the financial text
        :return: String without multiple newlines
        """

        text = re.sub(r'(( )*\n( )*){2,}', '#NEWLINE', text)
        text = re.sub(r'\n', ' ', text)
        text = re.sub(r'(#NEWLINE)+', '\n', text).strip()
        text = re.sub(r'[ ]{2,}', ' ', text)

        return text

    @staticmethod
    def strip_html(html_content):
        html_content = re.sub(r'(<\s*/\s*(div|tr|p|li|)\s*>)', r'\1\n\n', html_content)
        html_content = re.sub(r'(<br\s*>|<br\s*/>)', r'\1\n\n', html_content)
        html_content = re.sub(r'(<\s*/\s*(th|td)\s*>)', r' \1 ', html_content)
        html_content = HtmlStripper().strip_tags(html_content)

        return html_content
    
    @staticmethod
    def clean_text(text):
        """
        Clean the text of various unnecessary blocks of text
        Substitute various special characters

        :param text: Raw text string
        :return: String containing normalized, clean text
        """

        text = re.sub(r'[\xa0]', ' ', text)
        text = re.sub(r'[\u200b]', ' ', text)

        text = re.sub(r'[\x91]', '‘', text)
        text = re.sub(r'[\x92]', '’', text)
        text = re.sub(r'[\x93]', '“', text)
        text = re.sub(r'[\x94]', '”', text)
        text = re.sub(r'[\x95]', '•', text)
        text = re.sub(r'[\x96]', '-', text)
        text = re.sub(r'[\x97]', '-', text)
        text = re.sub(r'[\x98]', '˜', text)
        text = re.sub(r'[\x99]', '™', text)

        text = re.sub(r'[\u2010\u2011\u2012\u2013\u2014\u2015]', '-', text)

        def remove_whitespace(match):
            ws = r'[^\S\r\n]'
            return f'{match[1]}{re.sub(ws, r"", match[2])}{match[3]}{match[4]}'

        # Fix broken section headers
        text = re.sub(r'(\n[^\S\r\n]*)(P[^\S\r\n]*A[^\S\r\n]*R[^\S\r\n]*T)([^\S\r\n]+)((\d{1,2}|[IV]{1,2})[AB]?)',
                        remove_whitespace, text, flags=re.IGNORECASE)
        text = re.sub(r'(\n[^\S\r\n]*)(I[^\S\r\n]*T[^\S\r\n]*E[^\S\r\n]*M)([^\S\r\n]+)(\d{1,2}[AB]?)',
                        remove_whitespace, text, flags=re.IGNORECASE)

        text = re.sub(r'(ITEM|PART)(\s+\d{1,2}[AB]?)([\-•])', r'\1\2 \3 ', text, flags=re.IGNORECASE)

        # Remove unnecessary headers
        text = re.sub(r'\n[^\S\r\n]*'
                        r'(TABLE\s+OF\s+CONTENTS|INDEX\s+TO\s+FINANCIAL\s+STATEMENTS|BACK\s+TO\s+CONTENTS|QUICKLINKS)'
                        r'[^\S\r\n]*\n',
                        '\n', text, flags=regex_flags)

        # Remove page numbers and headers
        text = re.sub(r'\n[^\S\r\n]*[-‒–—]*\d+[-‒–—]*[^\S\r\n]*\n', '\n', text, flags=regex_flags)
        text = re.sub(r'\n[^\S\r\n]*\d+[^\S\r\n]*\n', '\n', text, flags=regex_flags)

        text = re.sub(r'[\n\s]F[-‒–—]*\d+', '', text, flags=regex_flags)
        text = re.sub(r'\n[^\S\r\n]*Page\s[\d*]+[^\S\r\n]*\n', '', text, flags=regex_flags)

        return text

    def remove_html_tables(self, html_files):
        for key, value in html_files.items():
            soup = BeautifulSoup(value, 'lxml')
            tables = soup.find_all('table')
            if len(tables):
                for table in tables:
                    table.extract()
            html_files[key] = soup
        
        return html_files

    def extract_items(self, filing_metadata):
        """
        Extracts all items/sections for a A001 file and writes it to a json file

        :param filing_metadata: a pandas series containing all filings metadata
        """
        absolute_filename = os.path.join(self.raw_files_folder, filing_metadata['filename'])
        html_files = {}
        with open(absolute_filename, 'r', errors='backslashreplace') as file:
            lines  = file.readlines()
            current_html = ""
            current_filename = None

            for line in lines:
                if delimiter in line:
                    if current_filename is not None:
                        if check_roman_numerals(current_filename):
                            html_files[current_filename.strip()] = current_html

                    current_filename = line.split(delimiter)[1].strip().rstrip("-->")
                    current_html = ""
                else:
                    current_html += line
            if current_filename is not None and check_roman_numerals(current_filename):
                html_files[current_filename.strip()] = current_html

        if self.remove_tables:
            html_files = self.remove_html_tables(html_files)
        ##need 회사 정보 및 metadata?
        #if need -> pasrsing companies_info and add filing_metadata
        with open(os.path.join(DATASET_DIR, 'companies_info.json'), encoding="utf-8") as f:
            company_info_dict = json.load(fp=f)

        company_info = company_info_dict[filing_metadata['corp_code']]

        json_content = {
            "corp_code": filing_metadata['corp_code'],
            "company": filing_metadata['corp_name'],
            "stock_code": filing_metadata['stock_code'],
            "filing_type": filing_metadata['filing_types'],
            "filing_date": filing_metadata['rcept_dt'],
            "ceo_name": company_info['ceo_name'],
            "address": company_info['address'],
            "induty_code": company_info['induty_code'],
            "establish_date": company_info['establish_date'],
        }
        
        for item_index in self.items_to_extract:
            item_index = roman.toRoman(int(item_index))
            json_content[f'item_{item_index}'] = ''

        for key, value in html_files.items():
            value = ExtractItems.strip_html(str(value))
            value = ExtractItems.clean_text(value)
            value = ExtractItems.remove_multiple_lines(value)
            item_idx = key.split('.')[0]
            if json_content.get(f'item_{item_idx}', False)!=False:
                json_content[f'item_{item_idx}'] = value

        return json_content


    def process_filing(self, filing_metadata):
        json_filename = f'{filing_metadata["filename"].split(".")[0]}.json'
        absolute_json_filename = os.path.join(self.extracted_files_folder, json_filename)
        if self.skip_extracted_filings and os.path.exists(absolute_json_filename):
            return 0
        
        json_content = self.extract_items(filing_metadata)

        if json_content is not None:
            with open(absolute_json_filename, 'w') as filepath:
                json.dump(json_content, filepath, indent=4, ensure_ascii=False)

        return 1

def main():

    filings_metadata_filepath = os.path.join(DATASET_DIR, config['filings_metadata_file'])
    if os.path.exists(filings_metadata_filepath):
        filings_metadata_df = pd.read_csv(filings_metadata_filepath, dtype=str)
        filings_metadata_df = filings_metadata_df.drop_duplicates(subset=['filename']).reset_index(drop=True)
        filings_metadata_df = filings_metadata_df.replace({np.nan: None})
    else:
        print(f'No such file "{filings_metadata_filepath}"')
        return
    
    raw_filings_folder = os.path.join(DATASET_DIR, config['raw_filings_folder'])
    if not os.path.isdir(raw_filings_folder):
        print(f'No such directory: "{raw_filings_folder}')
        return
    
    extracted_filings_folder = os.path.join(DATASET_DIR, config['extracted_filings_folder'])

    if not os.path.isdir(extracted_filings_folder):
        os.mkdir(extracted_filings_folder)

    extraction = ExtractItems(
        remove_tables=config['remove_tables'],
        items_to_extract=config['items_to_extract'],
        raw_files_folder=raw_filings_folder,
        extracted_files_folder=extracted_filings_folder,
        skip_extracted_filings=config['skip_extracted_filings']
    )

    print("Starting extraction...\n")

    list_of_series = list(zip(*filings_metadata_df.iterrows()))[1]

    with ProcessPool(processes=1) as pool:
        processed = list(tqdm(
            pool.imap(extraction.process_filing, list_of_series),
            total=len(list_of_series),
            ncols=100
        ))

    print(f'\nItem extraction is completed successfully.')
    print(f'{sum(processed)} files were processed.')
    print(f'Extracted filings are saved to: {extracted_filings_folder}')

    

if __name__ == '__main__':
    main()
