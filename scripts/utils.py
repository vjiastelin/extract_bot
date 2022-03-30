from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.errors import HttpError
import tabula
import pandas as pd
import os
import gspread
import re
import numpy as np
from datetime import date
import psycopg2
from decouple import config
from sqlalchemy import select,inspect
from orm import AsicsList,AsicsPrices


dir_store = './tmp_storage'
pd.options.mode.chained_assignment = None

dsn = config('DSN')
SHEET_KEY = config('SHEET_KEY')

CREDENTIALS_FILE = r'python-spreeadsheet-projects-3375f9903aba.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']


def get_match(x,dict,reg_exp):
    tmp_row = re.sub(reg_exp,'',x.upper().replace(' ',''))
    val = dict[dict['search_name'] == tmp_row]['name'].values
    return val[0] if len(val) > 0 else None

def pg_upsert(table,conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=table.index,
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)


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
        cur.execute('delete from asics.asics_prices where price_date = %s',(price_date,))
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        raise error
    finally:
        if conn is not None:
            cur.close()
            conn.close()

def acics_price(file_name: str):
    chars_to_remove = ['-']
    regular_expression = '|'.join([re.escape(x.upper()) for x in chars_to_remove])
    df_dict = pd.read_sql("select name, upper(replace(name,' ','')) search_name from asics.asics_list",dsn)
    df_dict = df_dict.fillna('')
    try:
        tables = tabula.read_pdf(file_name, pages="all")
        df = buid_dataframe(tables,df_dict,regular_expression)
        if config('TO_DB',default=True,cast=bool):
            df.to_sql('asics_prices',dsn,if_exists='append',index=False,schema='asics')
        spread_sheet = update_worksheet(df)
        return spread_sheet
    except Exception as e:
        delete_from_db(date.today())
        raise e

def get_today_curr():
    import requests
    import json
    currency_api = f'https://currencyapi.com/api/v2/latest?apikey=a7ea9a90-9dda-11ec-a030-a3200f160e11'
    header = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
    }
    try:
        r = requests.get(currency_api, headers=header)
    except:
        raise Exception('Something wrong with currency api')
    data = json.loads(r.text)
    return data['data']['RUB']


def buid_dataframe(tables,df_dict,regular_expression):
    price_date = date.today().strftime('%Y-%m-%d')
    df = pd.DataFrame(columns=[column.name for column in inspect(AsicsPrices).c])
    # Drop empty columns and format data
    for i, table in enumerate(tables, start=1):
        # Drop empty and
        tmp_df = table.dropna(how='all', axis=1)
        # Shift columns name to first row
        tmp_df.loc[-1] = tmp_df.columns
        tmp_df.index = tmp_df.index + 1  # shifting index
        tmp_df = tmp_df.sort_index()
        checked_value = tmp_df[tmp_df.columns[1]].astype('str').str.extractall('([\d.]+)').unstack().fillna('').sum(axis=1).astype(int).tolist()[0]
        if checked_value < 100000:
            price_col = 'price_usd'
        # Rename columns
        tmp_df.rename(
            {tmp_df.columns[0]: 'asic_name_raw',tmp_df.columns[1]: price_col, tmp_df.columns[2]: 'price_cny'}, axis=1, inplace=True)
        df = df.append(tmp_df, ignore_index=True)
    df = df.replace([0.0,'0',0], np.nan)
    df = df.drop_duplicates(subset=['asic_name_raw', price_col])
 
    #Mark used and brandnew asics
    index = df[df['asic_name_raw'].str.contains("Б/У")].index
    if len(index) > 0:
        df.loc[index[0]+1:, 'used_flag'] = True
    df.loc[df['used_flag'] != True, 'used_flag'] = False

    #Format main price col and calc for exchange
    df[price_col] = pd.to_numeric(df[price_col],errors='coerce')
    df.dropna(subset=[price_col],inplace=True)

    currency = get_today_curr()

    if 'usd' in price_col:
        df['price_rub'] = (df['price_usd'] * currency).round(2)
    else:
        df['price_usd'] = (df['price_rub'] / currency).round(2)

    # Format cny column
    df['price_cny'] = df['price_cny'].apply(lambda x: x.replace(" ", "") if type(x) is str else x)
    df['price_cny'] = pd.to_numeric(df['price_cny'],errors='coerce')
    # Format asic_name_raw column
    df['asic_name_raw'] = df['asic_name_raw'].apply(lambda x: x.replace("▪", ""))
    #Insert price date
    df['price_date'] = price_date

    df['asic_name'] = df['asic_name_raw'].apply(lambda x: get_match(x,df_dict,regular_expression))
    df = df.groupby(['asic_name_raw']).max()
    df = df.reset_index(drop=True)
    return df


def update_worksheet(asics_pd : pd.DataFrame):
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE, SCOPES)

    gc = gspread.authorize(creds)

    try:
        sh = gc.open_by_key(SHEET_KEY)
        worksheet = sh.get_worksheet(0)
        worksheet.clear()     
        asics_pd['price_usd'] = asics_pd['price_usd'] * 1.1
        response = worksheet.update(
            [['Наименование','Цена (usdt)']] + asics_pd[asics_pd.used_flag == False][['asic_name_raw','price_usd']].values.tolist())
        endIdx = re.search(
            r'\d+', response['updatedRange'].split(':')[1]).group(0)
        worksheet.update(f'A{str(int(endIdx) + 1)}', 'Б/У')
        worksheet.update(f'A{str(int(endIdx) + 2)}',
                         asics_pd[asics_pd.used_flag == True][['asic_name_raw','price_usd']].values.tolist())
        worksheet.format('A1:B1', {'textFormat': {'bold': True}})

        return f'https://docs.google.com/spreadsheets/d/{SHEET_KEY}'

    except HttpError as e:
        print(e)

