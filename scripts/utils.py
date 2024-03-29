from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.errors import HttpError
import tabula
import pandas as pd
import os
import gspread
import re
import enum
import numpy as np
from datetime import date,datetime
import psycopg2
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML
from decouple import config
from sqlalchemy import select,inspect
from orm import AsicsList,AsicsPrices, GpuPrices


dir_store = './tmp_storage'
static_dir = './static'
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
        cur.execute('delete from asics.gpu_prices where price_date = %s',(price_date,))
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        raise error
    finally:
        if conn is not None:
            cur.close()
            conn.close()

def acics_price(file_name: str,original_name: str):
    df_gpu = pd.DataFrame(columns=[column.name for column in inspect(GpuPrices).c])   
    chars_to_remove = ['-']
    regular_expression = '|'.join([re.escape(x.upper()) for x in chars_to_remove])
    df_dict = pd.read_sql("select name, upper(replace(name,' ','')) search_name from asics.asics_list",dsn)
    df_dict = df_dict.fillna('')
    mathces = re.search('(\d{2}_\d{2}_\d{1,2})',original_name)
    price_date = datetime.strptime(mathces[0],'%d_%m_%y')
    currency = get_today_curr()
    try:
        page2 = pd.DataFrame()
        table = tabula.read_pdf_with_template(file_name, pages=1,stream=True, template_path='first-page-template.json')[0]
        table.rename({table.columns[0]: 'column1',table.columns[1]: 'column2',table.columns[2]: 'column3'}, axis=1, inplace=True)
        try:   
            page2 = tabula.read_pdf_with_template(file_name, pages=2,stream=True, template_path='second-page-template.json')[0]
            page2.loc[-1] = page2.columns
            page2.index = page2.index + 1  # shifting index
            page2 = page2.sort_index()
            page2.rename({page2.columns[0]: 'column1',page2.columns[1]: 'column2',page2.columns[2]: 'column3'}, axis=1, inplace=True)
           
        except Exception as e:
            pass 
        table = table.append(page2, ignore_index=True)
        df = buid_dataframes(table,df_dict,regular_expression,price_date,currency)
        df_gpu[['gpu_name_raw','price_usd','price_rub','price_date']] = df[df.gpu == True][['asic_name_raw','price_usd','price_rub','price_date']]
        if config('TO_DB',default=True,cast=bool):
            df[df.gpu != True][df.columns.difference(['gpu'])].to_sql('asics_prices',dsn,if_exists='append',index=False,schema='asics')
            df_gpu.to_sql('gpu_prices',dsn,if_exists='append',index=False,schema='asics')
        #spread_sheet = update_worksheet(df)
        pdf_file = os.path.join(dir_store,"Прайс-лист ПРЕДЗАКАЗ {}.pdf".format(price_date.strftime('%d.%m.%Y')))
        create_pdf(df, pdf_file,price_date,currency)
        return pdf_file
    except Exception as e:
        delete_from_db(date.today())
        raise e

def get_today_curr():
    import requests
    import json
    currency_api = f'https://garantex.io/api/v2/depth?market=usdtrub'
    header = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
    }
    try:
        r = requests.get(currency_api, headers=header)
    except:
        raise Exception('Something wrong with currency api')
    data = json.loads(r.text)
    return float(data['asks'][0]['price'])


def buid_dataframes(tables,df_dict,regular_expression,price_date,currency):
    df = pd.DataFrame(columns=[column.name for column in inspect(AsicsPrices).c])
    price_col = 'price_usd'
    price_col_num = 2
    # Drop empty columns and format data
    tmp_df = tables    
   
    #tmp_df[tmp_df.columns[1]][0] = np.nan        
    tmp_df = tmp_df.dropna(how='all', axis=1)
    tmp_df = tmp_df.dropna(how='all', axis=0)
            
    # checked_value = tmp_df[tmp_df.columns[1]].astype('str').str.extractall('([\d.]+)').unstack().fillna('').sum(axis=1).astype(int).tolist()[0]
    # if checked_value < 60000:
    #     price_col = 'price_rub'
    #     price_col_num = 1
    # Rename columns
    tmp_df.rename(
        {tmp_df.columns[0]: 'asic_name_raw',tmp_df.columns[price_col_num]: price_col}, axis=1, inplace=True)
    drop_columns = [col for col in tmp_df.columns if col not in ['asic_name_raw',price_col]]
    tmp_df.drop(drop_columns,axis=1, inplace=True)
    df = df.append(tmp_df, ignore_index=True)
    df = df.replace([0.0,'0',0], np.nan)
    df = df.drop_duplicates(subset=['asic_name_raw', price_col])
 
    #Mark used,brandnew asics and gpu asics
    index_used = df[~df['asic_name_raw'].isna() & df['asic_name_raw'].str.contains("Б/У")].index
    index_gpu = df[~df['asic_name_raw'].isna() & df['asic_name_raw'].str.contains("^Видео")].index
    df['used_flag'] = False
    if len(index_used) > 0:
        df.loc[index_used[0]+1:index_gpu[0] if len(index_gpu) > 0 else None, 'used_flag'] = True
    if len(index_gpu) > 0:
        df.loc[index_gpu[0]+1:, 'gpu'] = True
        df.loc[index_gpu[0]+1:, 'used_flag'] = None
        #df.drop(index_gpu[0], inplace=True)    

    #Format main price col and calc for exchange
    df[price_col] = pd.to_numeric(df[price_col],errors='coerce')
    df.dropna(subset=[price_col],inplace=True)

    if 'usd' in price_col:
        df['price_rub'] = (df['price_usd'] * currency).round(2)
    else:
        df['price_usd'] = (df['price_rub'] / currency).round(2)

    # Format cny column
    # df['price_cny'] = df['price_cny'].apply(lambda x: x.replace(" ", "") if type(x) is str else x)
    # df['price_cny'] = pd.to_numeric(df['price_cny'],errors='coerce')
    # Format asic_name_raw column
    df['asic_name_raw'] = df['asic_name_raw'].apply(lambda x: str(x).replace("▪", ""))
    #Insert price date
    df['price_date'] = price_date

    df['asic_name'] = df['asic_name_raw'].apply(lambda x: get_match(x,df_dict,regular_expression))
    df = df.groupby(['asic_name_raw']).max()
    df = df.reset_index()    
    return df

def create_pdf(df: pd.DataFrame, pdf_file_path: str, price_date: datetime, currency):
    df['price_usd'] = (df['price_usd'] * 1.1 + 100).astype(int)
    df.rename(
            {'asic_name_raw': 'Наименование','price_usd': 'Цена (USDT)'}, axis=1, inplace=True)
    style_list = ["bootstrap.min.css","report.css"]
    for idx,style in enumerate(style_list):
        style_path = os.path.join(static_dir,style)
        style_list[idx] = style_path
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template(static_dir + "/asics_template.html")
    template_vars = {"price_date" : price_date.strftime('%d.%m.%Y'),
                "currency": currency,
                "new_asics": df[df.used_flag == False][['Наименование','Цена (USDT)']].to_html(classes='table table-bordered',index=False, justify='left').replace("<thead>", "<thead class='table-dark'>"),
                "used_asics": df[df.used_flag == True][['Наименование','Цена (USDT)']].to_html(classes='table table-bordered',index=False, justify='left' ).replace("<thead>", "<thead class='table-dark'>"),
                "gpu": df[df.gpu == True][['Наименование','Цена (USDT)']].to_html(classes='table table-bordered',index=False, justify='left' ).replace("<thead>", "<thead class='table-dark'>")}
    html_out = template.render(template_vars)
    HTML(string=html_out).write_pdf(pdf_file_path,stylesheets=style_list)

def update_worksheet(asics_pd : pd.DataFrame):
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE, SCOPES)

    gc = gspread.authorize(creds)

    try:
        sh = gc.open_by_key(SHEET_KEY)
        worksheet = sh.get_worksheet(0)
        worksheet.clear()     
        asics_pd['price_usd'] = (asics_pd['price_usd'] * 1.1 + 100).round(0)
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

