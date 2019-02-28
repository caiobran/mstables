from bs4 import BeautifulSoup as bs
from datetime import date
from io import StringIO
import update as up
import pandas as pd
import numpy as np
import sqlite3
import time
import json
import zlib
import csv
import re
import sys

print(sys.argv)

# Main function
def fetched(db_file, stp = 500):

    # Create db connection
    up.print_('Please wait while the database is being queried ...')
    while True:
        try:
            conn = sqlite3.connect(db_file)
            cur = conn.cursor()
        except sqlite3.OperationalError:
            continue
        except:
            raise
        break

    # Get list of fetched urls from Fetched_urls
    cols = 'url_id, ticker_id, exch_id, fetch_date, source_text'
    sql = '''SELECT {} FROM Fetched_urls
        WHERE status_code = 200 AND source_text <> "" AND parsed = 0
        ORDER BY ticker_id asc, url_id desc'''
    sql = sql.format(cols)
    fetched = up.execute_db(cur, sql).fetchall()

    # Call parse functions based on current item
    start = time.time()
    stp = len(fetched)
    if len(fetched) > 0:
        for i in range(stp):

            # Unpack record from Fetched_urls
            api = fetched[i][0]
            ticker_id = fetched[i][1]
            exch_id = fetched[i][2]
            fetch_date = fetched[i][3]
            parsed = 1
            parse = True
            erase = True
            code = 200

            try:
                source_text = zlib.decompress(fetched[i][4]).decode()
            except zlib.error as e:
                msg = '\t*** Unzip ERROR at Ticker {}, Exch {} & API {}'
                print(msg.format(ticker_id, exch_id, api))
                print(e)
                parse = False
            except:
                raise


            # Print progress message
            msg = 'Parsing {}/{} (%{:.1f}) ...'
            up.print_(msg.format(i + 1, stp, 100 * (i + 1) / stp))

            if parse == True:
                if api in [1, 2, 3]:
                    code = parse_1(cur, ticker_id, exch_id, source_text)
                elif api == 4:
                    code = parse_2(cur, ticker_id, exch_id, source_text)
                elif api == 5:
                    code = parse_3(cur, ticker_id, exch_id, source_text)
                elif api == 6:
                    code = parse_4(cur, ticker_id, exch_id, source_text)
                elif api == 7:
                    code = parse_5(cur, ticker_id, exch_id, source_text)
                elif api == 8:
                    code = parse_6(cur, ticker_id, exch_id, source_text)
                elif api == 9:
                    code = parse_7(cur, ticker_id, exch_id, source_text)
                else:
                    code = parse_8(
                       cur, api, ticker_id, exch_id, source_text)

            # Erase source_text from Fetched_urls and update source_code
            if erase == True:
                dict1 = {
                    'status_code':code,
                    'parsed':parsed,
                    'source_text':'null'
                }
                dict2 = {
                    'url_id':api,
                    'ticker_id':ticker_id,
                    'exch_id':exch_id,
                    'fetch_date':fetch_date
                }
                sql = update_record('Fetched_urls', dict1, dict2)
                up.execute_db(cur, sql)

            if i % 1000 == 0 and i + 1 != stp:
                up.save_db(conn)

    up.save_db(conn)
    cur.close()
    conn.close()
    fetched = None


# Fech table from source html code
def gethtmltable(sp):

    tr_tags = sp.find_all('tr')
    table = []
    for tr in tr_tags:
        td_tags = tr.find_all(['th', 'td'])
        if len(td_tags) > 1:
            table.append([tag.text for tag in td_tags])

    return table


# https://www.morningstar.com/api/v2/search/securities/5/usquote-v2/
def parse_1(cur, ticker_id, exch_id, data):

    results = []
    try:
        js = json.loads(data)
        if js['m'][0]['n'] != 0:
            results = js['m'][0]['r']
    except Exception as e:
        #print('\n# ERROR API 1:', e, end=' ')
        #print('at {}:{}\n'.format(exch_id, ticker_id))
        return 0

    if results == []:
        return 0

    for result in results:
        # Read data from current result
        exch = result['OS01X']
        symbol = result['OS001']
        exch_sym = result['LS01Z']
        country = result['XI018']
        type = result['OS010']
        comp = result['OS01W']
        curr = result['OS05M']

        if exch_sym == '' or symbol == '':
            continue

        # Fetch id's for data from db and update tables
        # Tickers
        ticker_id = up.sql_insert_one_get_id(cur, 'Tickers', 'ticker', symbol)
        # Currencies
        curr_id = up.sql_insert_one_get_id(cur, 'Currencies', 'code', curr)
        # Companies
        comp_id = up.sql_insert_one_get_id(cur, 'Companies', 'company', comp)
        # Types
        type_id = up.sql_insert_one_get_id(cur, 'Types', 'type_code', type)
        # Countries
        country_id = up.sql_insert_one_get_id(cur,
            'Countries', 'a3_un', country)
        # Updated date
        update = date.today().strftime('%Y-%m-%d')
        date_id = up.sql_insert_one_get_id(cur, 'TimeRefs', 'dates', update)
        # Exchanges
        exch_id = up.sql_insert_one_get_id(cur,
            'Exchanges', 'exchange_sym', exch_sym)
        dict1 = {
            'exchange':exch,
            'exchange_sym':exch_sym,
            'country_id':country_id
        }
        sql = update_record('Exchanges', dict1, {'id':exch_id})
        up.execute_db(cur, sql)

        # Master Table
        columns = '(ticker_id, exchange_id)'
        sql = up.sql_insert('Master', columns, (ticker_id, exch_id))
        up.execute_db(cur, sql)
        dict1 = {
            'company_id':comp_id,
            'type_id':type_id,
            'update_date_id':date_id
            }
        dict2 = {
            'ticker_id':ticker_id,
            'exchange_id':exch_id
            }
        sql = update_record('Master', dict1, dict2)
        up.execute_db(cur, sql)

    return 200


# http://quotes.morningstar.com/stockq/c-company-profile?
def parse_2(cur, ticker_id, exch_id, data):

    try:
        soup = bs(data, 'html.parser')
        tags = soup.find_all('span')
        sector = tags[2].text.strip()
        industry = tags[4].text.strip()
        ctype = tags[6].text.strip()
        fyend = tags[10].text.strip()
        style = tags[12].text.strip()
    except Exception as e:
        #print('\n# ERROR API 2:', e, end=' ')
        #print('at {}:{}\n'.format(exch_id, ticker_id))
        return 0

    # Insert sector into Sectors
    sector_id = up.sql_insert_one_get_id(cur, 'Sectors', 'Sector', sector)

    # Insert industry into Industries
    sql = up.sql_insert('Industries',
        '(industry, sector_id)', (industry, sector_id))
    up.execute_db(cur, sql)
    sql = up.sql_record_id('Industries', '(industry)', industry)
    industry_id = up.execute_db(cur, sql).fetchone()[0]

    # Insert companytype into CompanyTypes
    ctype_id = up.sql_insert_one_get_id(
        cur, 'CompanyTypes', 'companytype', ctype)

    # Insert fyend into FYEnds
    fyend_id = up.sql_insert_one_get_id(cur, 'TimeRefs', 'dates', fyend)

    # Insert style into StockStyles
    style_id = up.sql_insert_one_get_id(cur, 'StockStyles', 'style', style)

    # Update Tickers table with parsed data
    sql = update_record('Master', {'industry_id':industry_id,
        'companytype_id':ctype_id, 'fyend_id':fyend_id, 'style_id':style_id},
        {'ticker_id':ticker_id, 'exchange_id':exch_id})
    up.execute_db(cur, sql)

    return 200


# http://quotes.morningstar.com/stockq/c-header?
def parse_3(cur, ticker_id, exch_id, data):

    try:
        soup = bs(data, 'html.parser')
        tags = soup.find_all('span') + soup.find_all('div')
    except Exception as e:
        #print('\n# ERROR API 3:', e, end=' ')
        #print('at {}:{}\n'.format(exch_id, ticker_id))
        return 0

    # Parse data into info dictionary
    info = {}

    for count, tag in enumerate(tags):

        attrs = tag.attrs
        text = re.sub('[\n\t]', '', tag.text.strip())
        text = re.sub('\s\s*', ' ', text)

        if text == '—' or text == '— mil' or text == '— bil':
            continue

        if attrs.get('vkey') == 'Currency':
            val = up.sql_insert_one_get_id(cur, 'Currencies', 'code', text)
            info['currency_id'] = val
        elif attrs.get('vkey') == 'OpenPrice':
            info['openprice'] = re.sub(',', '', text)
        elif attrs.get('vkey') == 'LastPrice':
            info['lastprice'] = re.sub(',', '', text)
        elif attrs.get('vkey') == 'DayRange':
            info['day_hi'] = re.sub(',', '', text.split('-')[0])
            info['day_lo'] = re.sub(',', '', text.split('-')[1])
        elif attrs.get('vkey') == '_52Week':
            info['_52wk_hi'] = re.sub(',', '', text.split('-')[0])
            info['_52wk_lo'] = re.sub(',', '', text.split('-')[1])
        elif attrs.get('vkey') == 'ProjectedYield':
            info['yield'] = re.sub('[%,]', '', text)
        elif attrs.get('vkey') == 'Volume':
            if ',' in text:
                text = float(re.sub(',', '', text))
            elif ' ' in text:
                s = text.find(' ')
                unit = 1
                if text[s + 1:] == 'mil':
                    unit = 10E6
                elif text[s + 1:] == 'bil':
                    unit = 10E9
                elif text[s + 1:] == 'tri':
                    unit = 10E12
                info['aprvol'] = float(text[:s]) * unit
        elif attrs.get('vkey') == 'AverageVolume':
            if ',' in text:
                text = float(re.sub(',', '', text))
            elif ' ' in text:
                s = text.find(' ')
                unit = 1
                if text[s + 1:] == 'mil':
                    unit = 10E6
                elif text[s + 1:] == 'bil':
                    unit = 10E9
                elif text[s + 1:] == 'tri':
                    unit = 10E12
                info['avevol'] = float(text[:s]) * unit
        elif attrs.get('gkey') == 'Forward':
            fpe = text
        elif attrs.get('vkey') == 'PE':
            info['fpe'] = re.sub(',', '', text)
        elif attrs.get('vkey') == 'PB':
            info['pb'] = re.sub(',', '', text)
        elif attrs.get('vkey') == 'PS':
            info['ps'] = re.sub(',', '', text)
        elif attrs.get('vkey') == 'PC':
            info['pc'] = re.sub(',', '', text)

    # Check if parsing was successful
    if info == {}:
        return 0

    if 'fpe' in locals() and fpe != 'Forward' and 'fpe' in info:
        del info['fpe']

    # Insert data into MSheader table
    info['ticker_id'] = ticker_id
    info['exchange_id'] = exch_id
    sql = up.sql_insert('MSheader',
        tuple(info.keys()), tuple(info.values()))
    cur.execute(sql)
    del info['ticker_id']
    del info['exchange_id']
    dict2 = {'ticker_id':ticker_id, 'exchange_id':exch_id}
    sql = update_record('MSheader', info, dict2)
    cur.execute(sql)

    '''with open('test/api3.html', 'w') as file:
        file.write(soup.prettify())'''

    return 200


# http://financials.morningstar.com/valuate/valuation-history.action?
def parse_4(cur, ticker_id, exch_id, data):

    info = {}
    def clean_val(h, v):
        if v != '—':
             info[h] = v

    try:
        soup = bs(data, 'html.parser')
        table = gethtmltable(soup)
        script = soup.find('script').text
    except Exception as e:
        #print('\n# ERROR API 4:', e, end=' ')
        #print('at {}:{}\n'.format(exch_id, ticker_id))
        return 0

    script = re.sub('[ \n\t]|\\n|\\t', '', script)
    script = re.findall('\[\[.+?\]\]', script)[0]
    columns = json.loads(script)

    # Parse Yr Columns
    for year, column in enumerate(columns):
        if column[0] % 2 == 0:
            yr = column[1]
            yr_id = up.sql_insert_one_get_id(cur, 'TimeRefs', 'dates', yr)
            header = 'Y{}'.format(int((year-1)/2))
            info[header] = yr_id

    # Parse 'Price/Earnings'
    for yr, val in enumerate(table[1][1:]):
        header = 'PE_Y{}'.format(yr)
        clean_val(header, val)

    # Parse 'Price/Book'
    for yr, val in enumerate(table[4][1:]):
        header = 'PB_Y{}'.format(yr)
        clean_val(header, val)

    # Parse 'Price/Sales'
    for yr, val in enumerate(table[7][1:]):
        header = 'PS_Y{}'.format(yr)
        clean_val(header, val)

    # Parse 'Price/Cash Flow'
    for yr, val in enumerate(table[10][1:]):
        header = 'PC_Y{}'.format(yr)
        clean_val(header, val)

    # Check if parsing was successful
    if info == {}:
        return 0

    # Insert data into MSvaluation table
    info['ticker_id'] = ticker_id
    info['exchange_id'] = exch_id
    sql = up.sql_insert('MSvaluation',
        tuple(info.keys()), tuple(info.values()))
    cur.execute(sql)
    del info['ticker_id']
    del info['exchange_id']
    dict2 = {'ticker_id':ticker_id, 'exchange_id':exch_id}
    sql = update_record('MSvaluation', info, dict2)
    cur.execute(sql)

    # Check if parsing was successful
    if info == {}:
        return 0

    return 200


# http://financials.morningstar.com/finan/financials/getKeyStatPart.html?
def parse_5(cur, ticker_id, exch_id, data):

    try:
        html = json.loads(data)['componentData']
        soup = bs(html, 'html.parser')
    except Exception as e:
        #print('\n# ERROR API 5:', e, end=' ')
        #print('at {}:{}\n'.format(exch_id, ticker_id))
        return 0

    # Parse data batabase tables (5 tables)
    tabs = soup.find_all('div')
    tables = {}

    for tab in tabs:
        if 'id' in tab.attrs:
            info = {}
            #info0 = {}
            table = re.sub('tab-', 'MSratio_', tab['id'])

            '''# Parse data into info dictionary
            with open('test/{}.json'.format(table)) as file:
                info0 = json.load(file)'''

            trs = tab.find_all('tr')
            for tr in trs:
                tags = tr.find_all(['th', 'td'])
                for ct, tag in enumerate(tags):

                    # Parse column and row headers
                    if 'id' in tag.attrs:
                        if ct == 0:
                            text_id = text_id = up.sql_insert_one_get_id(
                                cur, 'RowHeaders', 'header', tag.text)
                        else:
                            text_id = up.sql_insert_one_get_id(
                                cur, 'TimeRefs', 'dates', tag.text)
                        key = re.sub('-', '_', tag['id'])
                        info[key] = int(text_id)
                        #info0[tag['id']] = 'INTEGER,'

                    # Parse values
                    if 'headers' in tag.attrs:
                        col = tag['headers']
                        col = '{}_{}'.format(col[2], col[0])
                        col = re.sub('-', '_', col)
                        try:
                            info[col] = float(tag.text)
                        except:
                            pass
                        #info0[col] = 'REAL,'

            if info != {}:
                # Insert data into tables
                info['ticker_id'] = ticker_id #'INTEGER,'
                info['exchange_id'] = exch_id  #'INTEGER,'
                tables[table] = info
                sql = up.sql_insert(table, tuple(info.keys()),
                    tuple(info.values()))
                cur.execute(sql)
                del info['ticker_id']
                del info['exchange_id']
                dict2 = {'ticker_id':ticker_id, 'exchange_id':exch_id}
                sql = update_record(table, info, dict2)
                cur.execute(sql)

            '''with open('test/{}.json'.format(table), 'w') as file:
                file.write(json.dumps(info0, indent=2))'''

    # Check if parsing was successful
    if tables == {}:
        return 0

    '''with open('test/api11.html', 'w') as file:
        file.write(soup.prettify())'''

    '''for k0, v0 in tables.items():
        print(k0)
        for k1, v1 in v0.items():
            print(k1, '\t', v1)'''

    return 200


# http://financials.morningstar.com/finan/financials/getFinancePart.html?
def parse_6(cur, ticker_id, exch_id, data):

    try:
        html = json.loads(data)['componentData']
        soup = bs(html, 'html.parser')
        trs = soup.find_all('tr')
    except Exception as e:
        #print('\n# ERROR API 6:', e, end=' ')
        #print('at {}:{}\n'.format(exch_id, ticker_id))
        return 0

    # Parse table
    info = {}
    #info0 = {}

    #with open('test/MSfinancials.json') as file:
    #    info0 = json.load(file)

    for tr in trs:
        tags = tr.find_all(['th', 'td'])

        for ct, tag in enumerate(tags):

            # Parse column and row headers
            if 'id' in tag.attrs:
                if ct != 0:
                    text_id = up.sql_insert_one_get_id(
                        cur, 'TimeRefs', 'dates', tag.text)
                else:
                    text = re.findall('\>(.+?)\<', str(tag))[0]
                    text = re.sub('\%|\*', '', text).strip()
                    text = re.sub('\s', '_', text)
                    text_id = up.sql_insert_one_get_id(
                        cur, 'RowHeaders', 'header', text)
                key = re.sub('-', '_', tag['id'])
                info[key] = int(text_id)
                #info0[tag['id']] = 'INTEGER,'

            # Parse values
            if 'headers' in tag.attrs:
                headers = tag['headers']
                text = '_'.join([headers[1], headers[0]])
                text = re.sub('[-,]', '_', text)
                try:
                    info[text] = float(tag.text)
                except:
                    pass
                #info0[text] = 'REAL,'

    if info == {}:
        return 0

    # Insert data into tables
    table = 'MSfinancials'
    info['ticker_id'] = ticker_id #'INTEGER,'
    info['exchange_id'] = exch_id  #'INTEGER,'
    sql = up.sql_insert(table, tuple(info.keys()), tuple(info.values()))
    cur.execute(sql)
    del info['ticker_id']
    del info['exchange_id']
    dict2 = {'ticker_id':ticker_id, 'exchange_id':exch_id}
    sql = update_record(table, info, dict2)
    cur.execute(sql)

    '''for k, v in info.items():
        print(k, v)'''

    '''with open('test/MSfinancials.json', 'w') as file:
        file.write(json.dumps(info0, indent=2))'''

    '''with open('test/api12.html', 'w') as file:
        file.write(soup.prettify())'''

    return 200


# http://performance.mor.../perform/Performance/stock/exportStockPrice.action?
def parse_7(cur, ticker_id, exch_id, data):

    # Calculate current moving averages
    if 'Morningstar.com Error Page' in data or data == None or data == '':
        return 0

    try:
        tbl = pd.read_csv(StringIO(data), sep=',', header=1)
        tbl = tbl.where(tbl['Volume'] != '???')
    except Exception as e:
        print(data, '\n\t', e)
        raise

    ave_50d = float(np.average(tbl.loc[:50, 'Close'].values))
    ave_100d = float(np.average(tbl.loc[:100, 'Close'].values))
    ave_200d = float(np.average(tbl.loc[:200, 'Close'].values))
    table = 'MSpricehistory'
    columns = '''(ticker_id, exchange_id, price_10yr,
        ave_50d, ave_100d, ave_200d)'''
    prices = zlib.compress(data.encode())

    # Insert record
    sql = 'INSERT OR IGNORE INTO {} {} VALUES (?, ?, ?, ?, ?, ?)'
    cur.execute(sql.format(table, columns),
        (ticker_id, exch_id, prices, ave_50d, ave_100d, ave_200d))

    # Update record
    sql = '''UPDATE {} SET price_10yr = ?, ave_50d = ?, ave_100d = ?,
        ave_200d = ? WHERE ticker_id = ? AND exchange_id = ?'''
    cur.execute(sql.format(table),
        (prices, ave_50d, ave_100d, ave_200d, ticker_id, exch_id))

    '''with open('test/api7.csv', 'w') as file:
        file.write(data)'''

    return 200


# http://financials.morningstar.com/ajax/ReportProcess4HtmlAjax.html?
def parse_8(cur, api, ticker_id, exch_id, data):

    try:
        js = json.loads(data)
        html = js['result']
        soup = bs(html, 'html.parser')
        tags = soup.find_all('div')
    except Exception as e:
        #print('\n# ERROR API {}:'.format(api), e, end=' ')
        #print('at {}:{}\n'.format(exch_id, ticker_id))
        return 0

    info = {}
    #info0 = {}
    type = 'MSreport'

    if api in [10, 11]:
        type += '_is'
    elif api in [12, 13]:
        type += '_cf'
    elif api in [14, 15]:
        type += '_bs'
    if api in [10, 12, 14]:
        type += '_yr'
    elif api in [11, 13, 15]:
        type += '_qt'
    #fname = 'test/{}.json'.format(type)

    '''with open(fname) as file:
        info0 = json.load(file)'''

    # Parse data into info dictionary
    for tag in tags:
        attrs = tag.attrs
        if 'id' in attrs:
            tag_id = tag['id']
            value = tag.text

            # Parse Yrly or Qtrly values
            if tag_id[:2] == 'Y_':
                parent = tag.parent['id']
                key = '{}_{}'.format(parent, tag_id)

                if 'rawvalue' in attrs:
                    if tag['rawvalue'] in ['—', 'nbsp']:
                        continue
                    info[key] = float(re.sub(',', '', tag['rawvalue']))
                    #info0[key] = 'REAL,'
                else:
                    if 'title' in attrs:
                        value = tag['title']
                    value_id = up.sql_insert_one_get_id(
                        cur, 'TimeRefs', 'dates', value)
                    info[key] = value_id
                    #info0[key] = 'INTEGER,'


            # Parse labels
            elif tag_id[:3] == 'lab' and 'padding' not in tag_id:
                value_id = up.sql_insert_one_get_id(
                    cur, 'RowHeaders', 'header', value)
                info[tag_id] = value_id
                #info0[tag_id] = 'INTEGER,'

    # Check if parsing was successful
    if info == {} and info0 == {}:
        return 0

    # Insert data into tables
    info['ticker_id'] = ticker_id
    info['exchange_id'] = exch_id
    sql = up.sql_insert(type, tuple(info.keys()), tuple(info.values()))
    cur.execute(sql)
    del info['ticker_id']
    del info['exchange_id']
    dict2 = {'ticker_id':ticker_id, 'exchange_id':exch_id}
    sql = update_record(type, info, dict2)
    cur.execute(sql)

    '''with open(fname, 'w') as file:
        file.write(json.dumps(info0, indent=2))'''

    return 200


# Generate UPDATE SQL command
def update_record(table, dict1, dict2):
    updates = str(dict1).replace('{\'', '').replace(', \'', ', ')
    updates = updates.replace('}', '').replace('\':', ' =')
    conds = str(dict2).replace('{\'', '(')
    conds = conds.replace('}', ')').replace('\':', ' =')
    conds = conds.replace(', \'', ' AND ')
    sql = 'UPDATE ' + table + ' SET ' + updates + ' WHERE ' + conds
    sql = re.sub('\'null\'', 'null', sql)
    return sql
