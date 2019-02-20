from bs4 import BeautifulSoup as bs
from tables import tables
import update as up
import sqlite3
import time
import json
import zlib
import re


# Main function
def fetched(stp = 0):

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
        WHERE status_code = 200 AND source_text <> ""
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
            source_text = fetched[i][4]
            code = 200

            # Print progress message
            msg = 'Parsing {}/{} (%{:.1f}) ...'
            up.print_(msg.format(i + 1, stp, 100 * (i + 1) / stp))

            erase = False
            if api == 1:
                code = parse_api_1(cur, ticker_id, exch_id, source_text)
                erase = True
            elif api == 2:
                code = parse_api_2(cur, ticker_id, exch_id, source_text)
                erase = True
            elif api == 3:
                code = parse_api_3(cur, ticker_id, exch_id, source_text)
                erase = True
            elif api == 4:
                code = parse_api_4(cur, ticker_id, exch_id, source_text)
                erase = True
            elif api == 5:
                code = parse_api_5(cur, ticker_id, exch_id, source_text)
                erase = True
            elif api == 6:
                code = parse_api_6(cur, ticker_id, exch_id, source_text)
                erase = True
            elif api == 7:
                code = parse_api_7(cur, ticker_id, exch_id, source_text)
                erase = True
            #elif api in [8, 9, 10, 11, 12, 13]:
            #    code = parse_api_8to13(
            #       cur, api, ticker_id, exch_id, source_text)
            #    erase = True

            # Erase source_text from Fetched_urls and update source_code
            if erase == True:
                dict1 = {
                    'status_code':code,
                    'source_text':''
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
def parse_api_1(cur, ticker_id, exch_id, data):

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
        dict1 = {'company_id':comp_id, 'type_id':type_id}
        dict2 = {'ticker_id':ticker_id, 'exchange_id':exch_id}
        sql = update_record('Master', dict1, dict2)
        up.execute_db(cur, sql)

    return 200


# http://quotes.morningstar.com/stockq/c-company-profile?
def parse_api_2(cur, ticker_id, exch_id, data):

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
def parse_api_3(cur, ticker_id, exch_id, data):

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
            info['openprice'] = text
        elif attrs.get('vkey') == 'LastPrice':
            info['lastprice'] = text
        elif attrs.get('vkey') == 'DayRange':
            info['day_hi'] = text.split('-')[0]
            info['day_lo'] = text.split('-')[1]
        elif attrs.get('vkey') == '_52Week':
            info['_52wk_hi'] = text.split('-')[0]
            info['_52wk_lo'] = text.split('-')[1]
        elif attrs.get('vkey') == 'ProjectedYield':
            info['yield'] = re.sub('%', '', text)
        elif attrs.get('vkey') == 'Volume':
            if ',' in text:
                info['aprvol'] = float(re.sub(',', '', text))
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
                info['avevol'] = float(re.sub(',', '', text))
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
            info['fpe'] = text
        elif attrs.get('vkey') == 'PB':
            info['pb'] = text
        elif attrs.get('vkey') == 'PS':
            info['ps'] = text
        elif attrs.get('vkey') == 'PC':
            info['pc'] = text

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
def parse_api_4(cur, ticker_id, exch_id, data):

    try:
        soup = bs(data, 'html.parser')
        table = gethtmltable(soup)
    except Exception as e:
        #print('\n# ERROR API 4:', e, end=' ')
        #print('at {}:{}\n'.format(exch_id, ticker_id))
        return 0

    # Parse Yr column Headers
    info = {}
    script = soup.find('script').text
    script = re.sub('[\n\t]', '', script)
    script = re.findall('\[\[.+?\]\]', script)[0]
    columns = json.loads(script)

    for year, column in enumerate(columns):
        if column[0] % 2 == 0:
            yr = column[1]
            yr_id = up.sql_insert_one_get_id(cur, 'TimeRefs', 'dates', yr)
            header = 'Y{}'.format(int((year-1)/2))
            info[header] = yr_id

    def clean_val(h, v):
        if v != '—':
             info[h] = v

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
def parse_api_5(cur, ticker_id, exch_id, data):

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
                                cur, 'RowHeaders', 'ticker', tag.text)
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
def parse_api_6(cur, ticker_id, exch_id, data):
    #print('\n***\n{}:{}\n***'.format(exch_id, ticker_id))

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
                        cur, 'RowHeaders', 'ticker', text)
                key = re.sub('-', '_', tag['id'])
                info[key] = int(text_id)
                #info0[tag['id']] = 'INTEGER,'

            # Parse values
            if 'headers' in tag.attrs:
                headers = tag['headers']
                text = '_'.join([headers[1], headers[0]])
                text = re.sub('-', '_', text)
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
def parse_api_7(cur, ticker_id, exch_id, data):

    zipped_vals = zlib.compress(data.encode())
    table = 'MSpricehistory'
    columns = '(ticker_id, exchange_id, price_10yr)'
    sql = 'INSERT INTO {} {} VALUES (?, ?, ?)'.format(table, columns)
    cur.execute(sql, (ticker_id, exch_id, zipped_vals))

    sql = '''UPDATE {} SET price_10yr = ?
        WHERE ticker_id = ? AND exchange_id = ?'''.format(table)
    cur.execute(sql, (zipped_vals, ticker_id, exch_id))

    '''with open('test/api7.csv', 'w') as file:
        file.write(data)'''

    return 200


# http://financials.morningstar.com/ajax/ReportProcess4HtmlAjax.html?
def parse_api_8to13(cur, api, ticker_id, exch_id, data):

    try:
        js = json.loads(data)
        html = js['result']
        soup = bs(html, 'html.parser')
        tags = soup.find_all('div')
    except Exception as e:
        print('\n# ERROR API {}:'.format(api), e, end=' ')
        print('at {}:{}\n'.format(exch_id, ticker_id))
        return 0

    # Parse data into info dictionary
    info = {}
    type = ''
    if api in [5, 6]:
        type += '_is'
    elif api in [7, 8]:
        type += '_cf'
    elif api in [9, 10]:
        type += '_bs'
    if api in [5, 7, 9]:
        type += '_yr'
    elif api in [6, 8, 10]:
        type += '_qt'

    fname = 'test/MSreport{}.json'.format(type)
    with open(fname) as file:
        info0 = json.load(file)

    for tag in tags:
        attrs = tag.attrs
        if 'id' in attrs:
            tag_id = tag['id']
            value = 'TEXT,' #tag.text
            rep = ''

            # Parse Yrly or Qtrly values
            if tag_id[:2] == 'Y_':
                parent = tag.parent['id']
                value0 = 'REAL,'

                if 'rawvalue' in attrs:
                    if api in [5, 6]:
                        '''
                        rep += 'is_'
                    elif api in [7, 8]:
                        rep += 'cf_'
                    elif api in [9, 10]:
                        rep += 'bs_'
                        '''
                    try:
                        value = float(tag['rawvalue'])
                    except:
                        value = 0.0

                key = '{}{}_{}'.format(rep, parent, tag_id)
                info[key] = value
                info0[key] = value0

            # Parse labels
            elif tag_id[:3] == 'lab' and 'padding' not in tag_id:

                key = '{}{}'.format(rep, tag_id)
                info[key] = value
                info0[key] = 'TEXT,'

    # Check if parsing was successful
    if info == {}:
        return 0

    # Insert data into tables
    info['ticker_id'] = ticker_id
    info['exchange_id'] = exch_id

    '''for k, v in info.items():
        print(k, v)'''

    with open(fname, 'w') as file:
        file.write(json.dumps(info0, indent=2))

    return 200


def update_record(table, dict1, dict2):
    updates = str(dict1).replace('{\'', '').replace(', \'', ', ')
    updates = updates.replace('}', '').replace('\':', ' =')
    conds = str(dict2).replace('{\'', '(')
    conds = conds.replace('}', ')').replace('\':', ' =')
    conds = conds.replace(', \'', ' AND ')
    sql = 'UPDATE ' + table + ' SET ' + updates + ' WHERE ' + conds
    return sql


db_file = 'db/equitable.sqlite'
tables  = tables('input/tables.json')
