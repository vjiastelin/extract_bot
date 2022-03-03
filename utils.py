from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.errors import HttpError
import tabula
import pandas as pd
import os
import gspread
import re
import difflib
import numpy as np
from datetime import date
import psycopg2

dir_store = './tmp_storage'
pd.options.mode.chained_assignment = None

dsn = 'postgresql://hkgykgvuodbtsn:8538ef37160181bc37d75fce56bbb4ed3fcfae1f889c825176387ed51d5587e0@ec2-34-250-92-138.eu-west-1.compute.amazonaws.com:5432/dn07ivjktvfkr'

CREDENTIALS_FILE = r'python-spreeadsheet-projects-3375f9903aba.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

SHEET_KEY = '1q1UZjcg9Il5LNPrEbyf0ZX0M36tMyBYBXx9KLcG6g3g'
# SHEET_KEY = '12TbKrKM_DBpFxMrsnDaQAl8WLKaUur1qPzJrBfD3xUg'


def pdf_to_excel(file_name: str, username: str):
    tables = tabula.read_pdf(file_name, pages="all")
    output_files = []
    for i, table in enumerate(tables):
        table.to_excel(os.path.join(dir_store, username) +
                       f'{i}_file.xlsx', index=False)
        output_files.append(os.path.join(
            dir_store, username) + f'{i}_file.xlsx')
    return output_files

def delete_from_db(price_date):
    try:
        conn = psycopg2.connect(dsn)        
        cur = conn.cursor()
        cur.execute('delete from asics_prices where price_date = %s',(price_date,))

        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        raise error
    finally:
        if conn is not None:
            cur.close()
            conn.close() 

def acics_price(file_name: str):    
    try:
        tables = tabula.read_pdf(file_name, pages="all")
        df = buid_dataframe(tables)
        df.to_sql('asics_prices',dsn,if_exists='append',index=False)
        spread_sheet = update_worksheet(df)
        return spread_sheet
    except Exception as e:
        delete_from_db(date.today())
        raise e
    


def buid_dataframe(tables):
    price_date = date.today().strftime('%Y-%m-%d')
    df = pd.read_sql('SELECT * FROM asics_prices where 1=0', dsn)
    # Drop empty columns and format data
    for i, table in enumerate(tables, start=1):
        # Drop empty and
        tmp_df = table.dropna(how='all', axis=1)
        # Shift columns name to first row
        tmp_df.loc[-1] = tmp_df.columns
        tmp_df.index = tmp_df.index + 1  # shifting index
        tmp_df = tmp_df.sort_index()
        # Rename columns
        tmp_df.rename(
            {tmp_df.columns[0]: 'asic_name_raw', tmp_df.columns[1]: 'price_rub', tmp_df.columns[2]: 'price_cny'}, axis=1, inplace=True)
        df = df.append(tmp_df, ignore_index=True)
    df = df.replace([0.0,'0',0], np.nan)
    df = df.drop_duplicates(subset=['asic_name_raw', 'price_rub'])
    index = df[df['asic_name_raw'].str.contains("Б/У")].index[0]

    #Mark used and brandnew asics
    df.loc[index+1:, 'used_flag'] = True
    df.loc[df['used_flag'] != True, 'used_flag'] = False

    df['price_rub'] = pd.to_numeric(df['price_rub'],errors='coerce')
    df.dropna(subset=['price_rub'],inplace=True)
    # Format cny column
    df['price_cny'] = df['price_cny'].apply(lambda x: x.replace(" ", "") if type(x) is str else x)
    df['price_cny'] = pd.to_numeric(df['price_cny'],errors='coerce')
    # Format asic_name_raw column
    df['asic_name_raw'] = df['asic_name_raw'].apply(lambda x: x.replace("▪", ""))
    #Paste date for price
    df['price_date'] = price_date

    df_al = pd.read_sql('SELECT name FROM asics_list', dsn)
   
    def get_match(x):
        match = difflib.get_close_matches(x.upper(),df_al['name'],1,0.75)
        return match[0] if len(match) > 0 else None

    df['asic_name'] = df['asic_name_raw'].apply(lambda x: get_match(x))

    return df


def update_worksheet(asics_pd : pd.DataFrame):
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE, SCOPES)

    gc = gspread.authorize(creds)

    try:
        sh = gc.open_by_key(SHEET_KEY)
        worksheet = sh.get_worksheet(0)
        worksheet.clear()
        response = worksheet.update(
            [['Наименование', 'Цена']] + asics_pd[asics_pd.used_flag == False][['asic_name_raw', 'price_rub']].values.tolist())
        endIdx = re.search(
            r'\d+', response['updatedRange'].split(':')[1]).group(0)
        worksheet.update(f'A{str(int(endIdx) + 1)}', 'Б/У')
        worksheet.update(f'A{str(int(endIdx) + 2)}',
                         asics_pd[asics_pd.used_flag == True][['asic_name_raw', 'price_rub']].values.tolist())
        worksheet.format('A1:B1', {'textFormat': {'bold': True}})

        return f'https://docs.google.com/spreadsheets/d/{SHEET_KEY}'

    except HttpError as e:
        print(e)
