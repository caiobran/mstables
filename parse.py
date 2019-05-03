from bs4 import BeautifulSoup as bs
from importlib import reload #Comment out once done using
import datetime as DT
from io import StringIO
import pandas as pd
import numpy as np
import fetch, sqlite3, time, json, zlib, csv, sys, re


# Main function
def parse(db_file):
    start = time.time()

    # Create db connection
    fetch.print_('Please wait while the database is being queried ...')

    while True:
        try:
            conn = sqlite3.connect(db_file)
            cur = conn.cursor()
        except sqlite3.OperationalError as S:
            fetch.print_('')
            print('\tError - sqlite3 error: {}'.format(S))
            continue
        except KeyboardInterrupt:
            print('\nGoodbye!')
            exit()
        except:
            raise
        break

    # Get list of fetched urls from Fetched_urls
    cols = 'url_id, ticker_id, exch_id, fetch_date, source_text'
    sql = '''SELECT {} FROM Fetched_urls
        WHERE status_code = 200 AND parsed = 0
        ORDER BY ticker_id asc, url_id desc'''
    sql = sql.format(cols)
    fetched = fetch.db_execute(cur, sql).fetchall()

    # Call parsing methods
    parsing(conn, cur, fetched)

    # Save db and close db connection
    fetch.save_db(conn)
    cur.close()
    conn.close()
    fetched = None


def parsing(conn, cur, items):
    stp = len(items)
    spds = []
    if stp > 0:
        for i in range(stp):

            # Unpack record from Fetched_urls
            api = items[i][0]
            ticker_id = items[i][1]
            exch_id = items[i][2]
            fetch_date = items[i][3]
            source_text = items[i][4]
            parse = True
            parsed = 1
            code = 200

            # Decompress and check data integrity before parsing
            try:
                if source_text is not None:
                    source_text = zlib.decompress(source_text).decode()
            except BaseException as B:
                print('\n\nB - {}'.format(str(B)))
                raise
            except Exception as E:
                print('\n\nE - {}'.format(str(E)))
                raise
            except KeyboardInterrupt:
                print('\nGoodbye!')
                exit()
            if (source_text is None or len(source_text) == 0 or
                'Morningstar.com Error Page' in source_text or
                'This page is temporarily unavailable' in source_text):
                parse = False
                source_text = 'null'
                parsed = 0
                code = 0

            # Print progress message
            msg = 'Parsing results into database...'
            msg += '\t{:6,.0f} / {:6,.0f}\t({:6.1%} )'
            ct = i + 1
            pct = (i + 1) / stp
            fetch.print_(msg.format(ct, stp, pct))

            # Invoke parsing function based on API number
            if parse == True:
                if api in [1, 2, 3]:
                    code = parse_1(cur, ticker_id, exch_id, source_text, api)
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
                elif api in [10, 11, 12, 13, 14, 15]:
                    code = parse_8(cur, api, ticker_id, exch_id, source_text)
                elif api == 16:
                    code = parse_9(cur, ticker_id, exch_id, source_text)
                elif api == 0:
                    pass
                    # code = parse_10(cur, ticker_id, exch_id, source_text)
                source_text = 'null'

            # Updated record in Fetched_urls with results from parsing
            if True:
                dict1 = {
                    'status_code':code,
                    'parsed':parsed,
                    'source_text':source_text
                }
                dict2 = {
                    'url_id':api,
                    'ticker_id':ticker_id,
                    'exch_id':exch_id,
                    'fetch_date':fetch_date
                }
                sql = update_record('Fetched_urls', dict1, dict2)
                fetch.db_execute(cur, sql)

            #print('\n{} SQL = {}\n\n'.format(api, sql))

            if i % 1000 == 0 and i + 1 != stp:
                fetch.save_db(conn)


def db_execute_tpl(cur, sql, tpl):
    while True:
        try:
            return cur.execute(sql,tpl)
        except sqlite3.OperationalError as S:
            fetch.print_('')
            print('\tError - sqlite3 error: {}'.format(S))
        except KeyboardInterrupt:
            print('\nGoodbye!')
            exit()
        except:
            print('\n\nSQL cmd = \'{}\'\n{}\n'.format(sql, tpl))
            raise


# Fech table from source html code
def get_html_table(sp):

    tr_tags = sp.find_all('tr')
    table = []
    for tr in tr_tags:
        td_tags = tr.find_all(['th', 'td'])
        if len(td_tags) > 1:
            table.append([tag.text for tag in td_tags])

    return table


# https://www.morningstar.com/api/v2/search/securities/5/usquote-v2/
def parse_1(cur, ticker_id, exch_id, data, api):

    results = []
    try:
        js = json.loads(data)
        if js['m'][0]['n'] != 0:
            results = js['m'][0]['r']
    except KeyError:
        fetch.print_('')
        print('\tError: KeyError at Parse_1\n')
        return 0
    except KeyboardInterrupt:
        print('\nGoodbye!')
        exit()
    except:
        print('\n\nTicker_id = {}, Exch_id = {}'.format(ticker_id, exch_id))
        print('Data = {} {}\n'.format(data, len(data)))
        raise

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
        ticker_id = fetch.sql_insert_one_get_id(
            cur, 'Tickers', 'ticker', symbol)

        # Currencies
        curr_id = fetch.sql_insert_one_get_id(
            cur, 'Currencies', 'currency_code', curr)

        # Companies
        comp_id = fetch.sql_insert_one_get_id(
            cur, 'Companies', 'company', comp)

        # SecurityTypes
        type_id = fetch.sql_insert_one_get_id(cur, 'SecurityTypes', 'security_type_code', type)

        # Countries
        country_id = fetch.sql_insert_one_get_id(cur,
            'Countries', 'a3_un', country)

        # Exchanges
        exch_id = fetch.sql_insert_one_get_id(cur,
            'Exchanges', 'exchange_sym', exch_sym)
        dict1 = {
            'exchange':exch,
            'exchange_sym':exch_sym,
            'country_id':country_id
        }
        sql = update_record('Exchanges', dict1, {'id':exch_id})
        fetch.db_execute(cur, sql)

        # Master Table
        columns = '(ticker_id, exchange_id)'
        sql = fetch.sql_insert('Master', columns, (ticker_id, exch_id))
        fetch.db_execute(cur, sql)
        dict1 = {
            'company_id':comp_id,
            'security_type_id':type_id,
            'update_date':DT.date.today().strftime('%Y-%m-%d')
            }
        dict2 = {
            'ticker_id':ticker_id,
            'exchange_id':exch_id
            }
        sql = update_record('Master', dict1, dict2)
        fetch.db_execute(cur, sql)

    return 200


# http://quotes.morningstar.com/stockq/c-company-profile
def parse_2(cur, ticker_id, exch_id, data):

    soup = bs(data, 'html.parser')
    tags = soup.find_all('span')

    try:
        sector = tags[2].text.strip()
        industry = tags[4].text.strip()
        stype = tags[6].text.strip()
        fyend = tags[10].text.strip()
        style = tags[12].text.strip()
    except KeyboardInterrupt:
        print('\nGoodbye!')
        exit()
    except:
        print('\n\nTicker_id = {}, Exch_id = {}'.format(ticker_id, exch_id))
        print('Data = {} {}\n'.format(data, len(data)))
        raise

    # Insert sector into Sectors
    sector_id = fetch.sql_insert_one_get_id(cur, 'Sectors', 'Sector', sector)

    # Insert industry into Industries
    sql = fetch.sql_insert('Industries',
        '(industry, sector_id)', (industry, sector_id))
    fetch.db_execute(cur, sql)
    sql = fetch.sql_record_id('Industries', '(industry)', industry)
    industry_id = fetch.db_execute(cur, sql).fetchone()[0]

    # Insert stock_type into StockTypes
    stype_id = fetch.sql_insert_one_get_id(
        cur, 'StockTypes', 'stock_type', stype)

    # Insert fyend into FYEnds
    fyend_id = fetch.sql_insert_one_get_id(cur, 'TimeRefs', 'dates', fyend)

    # Insert style into StockStyles
    style_id = fetch.sql_insert_one_get_id(cur, 'StockStyles', 'style', style)

    # Update Tickers table with parsed data
    sql = update_record('Master', {'industry_id':industry_id,
        'stock_type_id':stype_id, 'fyend_id':fyend_id, 'style_id':style_id},
        {'ticker_id':ticker_id, 'exchange_id':exch_id})
    fetch.db_execute(cur, sql)

    return 200


# http://quotes.morningstar.com/stockq/c-header
def parse_3(cur, ticker_id, exch_id, data):

    soup = bs(data, 'html.parser')
    tags = soup.find_all('span') + soup.find_all('div')

    info = {}
    noise = ['', '-', '—', '— mil', '— bil']

    # Parse data into info dictionary
    for count, tag in enumerate(tags):

        attrs = tag.attrs
        text = re.sub('[\n\t]', '', tag.text.strip())
        text = re.sub('\s\s*', ' ', text)

        try:
            if attrs.get('vkey') == 'Currency':
                if text in noise:
                    info['currency_id'] = 'null'
                else:
                    val = fetch.sql_insert_one_get_id(
                        cur, 'Currencies', 'currency_code', text)
                    info['currency_id'] = val

            elif attrs.get('vkey') == 'LastDate':
                if text == '':
                    info['lastdate'] = 'null'
                else:
                    info['lastdate'] = pd.to_datetime(
                        text).strftime('%Y-%m-%d')

            elif attrs.get('vkey') == 'DayRange':
                text = re.sub('^-0.00', '0.00', text)
                vals = text.split('-')
                if '-' not in text or text in noise or '' in vals:
                    info['day_lo'] = 'null'
                    info['day_hi'] = 'null'
                else:
                    info['day_lo'] = float(re.sub(',', '', vals[0]))
                    info['day_hi'] = float(re.sub(',', '', vals[1]))

            elif attrs.get('vkey') == '_52Week':
                text = re.sub('^-0.00', '0.00', text)
                vals = text.split('-')
                if '-' not in text or text in noise or '' in vals:
                    info['_52wk_lo'] = 'null'
                    info['_52wk_hi'] = 'null'
                else:
                    info['_52wk_lo'] = float(re.sub(',', '', vals[0]))
                    info['_52wk_hi'] = float(re.sub(',', '', vals[1]))

            elif attrs.get('vkey') == 'Volume':
                if text in noise:
                    info['lastvol'] = 'null'
                else:
                    text = re.sub(',', '', text)
                    unit = 1
                    if ' mil' in text:
                        unit = 10E6
                        text = text.replace(' mil', '')
                    elif ' bil' in text:
                        unit = 10E9
                        text = text.replace(' bil', '')
                    elif ' tri' in text:
                        unit = 10E12
                        text = text.replace(' tri', '')
                    info['lastvol'] = float(text) * unit

            elif attrs.get('vkey') == 'AverageVolume':
                if text in noise:
                    info['avevol'] = 'null'
                else:
                    text = re.sub(',', '', text)
                    unit = 1
                    if ' mil' in text:
                        unit = 10E6
                        text = text.replace(' mil', '')
                    elif ' bil' in text:
                        unit = 10E9
                        text = text.replace(' bil', '')
                    elif ' tri' in text:
                        unit = 10E12
                        text = text.replace(' tri', '')
                    info['avevol'] = float(text) * unit

            elif attrs.get('gkey') == 'Forward':
                fpe = text

            elif attrs.get('vkey') == 'OpenPrice':
                if text in noise:
                    info['openprice'] = 'null'
                else:
                    info['openprice'] = float(re.sub(',', '', text))

            elif attrs.get('vkey') == 'LastPrice':
                if text in noise:
                    info['lastprice'] = 'null'
                else:
                    info['lastprice'] = float(re.sub(',', '', text))

            elif attrs.get('vkey') == 'ProjectedYield':
                if text in noise:
                    info['yield'] = 'null'
                else:
                    info['yield'] = float(re.sub('[%,]', '', text))

            elif attrs.get('vkey') == 'PE':
                if text in noise:
                    info['fpe'] = 'null'
                else:
                    info['fpe'] = float(re.sub(',', '', text))

            elif attrs.get('vkey') == 'PB':
                if text in noise:
                    info['pb'] = 'null'
                else:
                    info['pb'] = float(re.sub(',', '', text))

            elif attrs.get('vkey') == 'PS':
                if text in noise:
                    info['ps'] = 'null'
                else:
                    info['ps'] = float(re.sub(',', '', text))

            elif attrs.get('vkey') == 'PC':
                if text in noise:
                    info['pc'] = 'null'
                else:
                    info['pc'] = float(re.sub(',', '', text))

        except:
            print('\n\n{' + text + '}\n')
            raise

    # Check if parsing was successful
    if info == {}:
        return 0

    if 'fpe' in locals() and fpe != 'Forward' and 'fpe' in info:
        del info['fpe']

    # Remove 'empty' string values
    for k, v in info.items():
        if v == '' or v == ' ':
            info[k] = 'null'

    # Insert data into MSheader table
    table = 'MSheader'
    # Update
    dict = {'ticker_id':ticker_id, 'exchange_id':exch_id}
    sql = update_record(table, info, dict)
    fetch.db_execute(cur, sql)
    # Insert
    if cur.rowcount == 0:
        info['ticker_id'] = ticker_id
        info['exchange_id'] = exch_id
        sql = fetch.sql_insert(table, tuple(info.keys()), tuple(info.values()))
        fetch.db_execute(cur, sql)

    return 200


# http://financials.morningstar.com/valuate/valuation-history.action
def parse_4(cur, ticker_id, exch_id, data):

    info = {}
    def clean_val(h, v):
        if v != '—':
             info[h] = v

    soup = bs(data, 'html.parser')
    table = get_html_table(soup)
    script = soup.find('script').text
    script = re.sub('[ \n\t]|\\n|\\t', '', script)
    script = re.findall('\[\[.+?\]\]', script)[0]
    columns = json.loads(script)

    # Parse Yr Columns
    for year, column in enumerate(columns):
        if column[0] % 2 == 0:
            yr = column[1]
            yr_id = fetch.sql_insert_one_get_id(cur, 'TimeRefs', 'dates', yr)
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
    table = 'MSvaluation'
    # Update
    dict = {'ticker_id':ticker_id, 'exchange_id':exch_id}
    sql1 = update_record(table, info, dict)
    # Insert
    info['ticker_id'] = ticker_id
    info['exchange_id'] = exch_id
    sql2 = fetch.sql_insert(table, tuple(info.keys()), tuple(info.values()))
    try:
        fetch.db_execute(cur, sql1)
        if cur.rowcount == 0:
            fetch.db_execute(cur, sql2)
    except sqlite3.OperationalError:
        pass
    except:
        raise

    # Check if parsing was successful
    if info == {}:
        return 0

    return 200


# http://financials.morningstar.com/finan/financials/getKeyStatPart.html
def parse_5(cur, ticker_id, exch_id, data):

    # Check if source data has correct information
    try:
        js = json.loads(data)['componentData']
        if js is None:
            return 0
        soup = bs(js, 'html.parser')
    except KeyboardInterrupt:
        print('\nGoodbye!')
        exit()
    except:
        print('\n\nTicker_id = {}, Exch_id = {}'.format(ticker_id, exch_id))
        print('Data = {} {}\n'.format(data, len(data)))
        raise

    # Parse table
    tables = {}
    trows = soup.find_all('tr')
    tname = ''
    for trow in trows:
        div_id = trow.parent.parent.parent.attrs['id']
        tname0 = re.sub('tab-', 'MSratio_', div_id)
        if tname != tname0:
            tname = tname0
            tables[tname] = {}

        row_tags = trow.find_all(['th', 'td'])
        for i, row_tag in enumerate(row_tags):
            if 'id' in row_tag.attrs:
                text = row_tag.text
                id = re.sub('-', '_', row_tag.attrs['id'])
                if i != 0:
                    text_id = fetch.sql_insert_one_get_id(
                        cur, 'TimeRefs', 'dates', text)
                else:
                    text_id = fetch.sql_insert_one_get_id(
                        cur, 'ColHeaders', 'header', text)
                tables[tname][id] = text_id
            elif 'headers' in row_tag.attrs:
                headers = row_tag.attrs['headers']
                header = '_'.join([headers[2], headers[0]])
                header = re.sub('-', '_', header)
                val = re.sub(',', '', row_tag.text)
                if val == '—':
                    val = None
                else:
                    val = float(val)
                tables[tname][header] = val

    # Check if parsing was successful
    if tables == {}:
        return 0

    # Insert data into tables
    for table in tables:
        # Update
        info = tables[table]
        dict = {'ticker_id':ticker_id, 'exchange_id':exch_id}
        sql = update_record(table, info, dict)
        fetch.db_execute(cur, sql)
        # Insert
        if cur.rowcount == 0:
            tables[table]['ticker_id'] = ticker_id
            tables[table]['exchange_id'] = exch_id
            info = tables[table]
            sql = fetch.sql_insert(
                table, tuple(info.keys()), tuple(info.values()))
            fetch.db_execute(cur, sql)

    return 200


# http://financials.morningstar.com/finan/financials/getFinancePart.html
def parse_6(cur, ticker_id, exch_id, data):

    # Check if source data has correct information
    try:
        js = json.loads(data)['componentData']
        if js is None:
            return 0
        soup = bs(js, 'html.parser')
    except KeyboardInterrupt:
        print('\nGoodbye!')
        exit()
    except:
        print('\n\nTicker_id = {}, Exch_id = {}'.format(ticker_id, exch_id))
        print('Data = {} {}\n'.format(data, len(data)))
        raise

    # Parse table
    table = {}
    trows = soup.find_all('tr')
    for trow in trows:
        row_tags = trow.find_all(['th', 'td'])
        for i, row_tag in enumerate(row_tags):
            if 'id' in row_tag.attrs:
                text = row_tag.text
                if i != 0:
                    text_id = fetch.sql_insert_one_get_id(
                        cur, 'TimeRefs', 'dates', text)
                else:
                    text_id = fetch.sql_insert_one_get_id(
                        cur, 'ColHeaders', 'header', text)
                table[row_tag.attrs['id']] = text_id
            elif 'headers' in row_tag.attrs:
                headers = row_tag.attrs['headers']
                headers.reverse()
                val = re.sub(',', '', row_tag.text)
                if val == '—':
                    val = None
                else:
                    val = float(val)
                table['_'.join(headers)] = val

    if table == {}:
        return 0

    # Insert data into tables
    tname = 'MSfinancials'
    # Update
    dict = {'ticker_id':ticker_id, 'exchange_id':exch_id}
    sql = update_record(tname, table, dict)
    ct = fetch.db_execute(cur, sql)
    # Insert
    if cur.rowcount == 0:
        table['ticker_id'] = ticker_id
        table['exchange_id'] = exch_id
        sql = fetch.sql_insert(
            tname, tuple(table.keys()), tuple(table.values()))
        fetch.db_execute(cur, sql)

    return 200


# http://performance.mor.../perform/Performance/stock/exportStockPrice.action
def parse_7(cur, ticker_id, exch_id, data):

    # Calculate current moving averages
    tbl = pd.read_csv(StringIO(data), sep=',', header=1)
    tbl = tbl.where(tbl['Volume'] != '???')

    ave_50d = float(np.average(tbl.loc[:50, 'Close'].values))
    ave_100d = float(np.average(tbl.loc[:100, 'Close'].values))
    ave_200d = float(np.average(tbl.loc[:200, 'Close'].values))
    table = 'MSpricehistory'
    columns = '''(ticker_id, exchange_id, price_10yr,
        ave_50d, ave_100d, ave_200d)'''
    prices = zlib.compress(data.encode())

    # Update record
    sql = '''UPDATE OR IGNORE {} SET price_10yr = ?, ave_50d = ?, ave_100d = ?,
        ave_200d = ? WHERE ticker_id = ? AND exchange_id = ?'''
    sql = sql.format(table)
    db_execute_tpl(cur, sql,
        (prices, ave_50d, ave_100d, ave_200d, ticker_id, exch_id))

    if cur.rowcount == 0:
        # Insert record
        sql = 'INSERT OR IGNORE INTO {} {} VALUES (?, ?, ?, ?, ?, ?)'
        sql = sql.format(table, columns)
        db_execute_tpl(cur, sql,
            (ticker_id, exch_id, prices, ave_50d, ave_100d, ave_200d))

    return 200


# http://financials.morningstar.com/ajax/ReportProcess4HtmlAjax.html
def parse_8(cur, api, ticker_id, exch_id, data):

    # Check if source data has correct information
    msg = 'There is no available information in our database to display.'
    if msg in data:
        return 0

    # Parse source data with JSON and BeautifulSoup
    try:
        js = json.loads(data)
        html = js['result']
        soup = bs(html, 'html.parser')
        tags = soup.find_all('div')
    except KeyboardInterrupt:
        print('\nGoodbye!')
        exit()
    except:
        print('\n\n', data)
        raise

    info = {}
    info0 = {}
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

            # Parse currency and FY End month number
            if tag_id == 'unitsAndFiscalYear':
                info['fye_month'] = int(tag['fyenumber'])
                curr_id = fetch.sql_insert_one_get_id(
                    cur, 'Currencies', 'currency_code', tag['currency'])
                info['currency_id'] = curr_id

            # Parse Yrly or Qtrly values
            elif tag_id[:2] == 'Y_':
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
                    value_id = fetch.sql_insert_one_get_id(
                        cur, 'TimeRefs', 'dates', value)
                    info[key] = value_id
                    #info0[key] = 'INTEGER,'

            # Parse labels
            elif tag_id[:3] == 'lab' and 'padding' not in tag_id:
                value_id = fetch.sql_insert_one_get_id(
                    cur, 'ColHeaders', 'header', value)
                info[tag_id] = value_id
                #info0[tag_id] = 'INTEGER,'

    # Check if parsing was successful
    if info == {} and info0 == {}:
        return 0

    # Insert data into tables
    # Update
    dict = {'ticker_id':ticker_id, 'exchange_id':exch_id}
    sql = update_record(type, info, dict)
    fetch.db_execute(cur, sql)
    # Insert
    if cur.rowcount == 0:
        info['ticker_id'] = ticker_id
        info['exchange_id'] = exch_id
        sql = fetch.sql_insert(type, tuple(info.keys()), tuple(info.values()))
        fetch.db_execute(cur, sql)

    return 200


# http://insiders.mor.../insiders/trading/insider-activity-data2.action
def parse_9(cur, ticker_id, exch_id, data):

    data = re.sub('([A-Z])', r' \1', data)
    data = re.sub(' +', ' ', data)
    data = re.sub('\\n|\\t', '', data)
    soup = bs(data, 'html.parser')
    table = get_html_table(soup)

    if len(table) > 1:
        for row in table:
            date = ''
            info = {}
            if row[0] != '':
                info['date'] = DT.datetime.strptime(
                    row[0], '%m/%d/%Y').strftime('%Y-%m-%d')
                try:
                    info['quantity'] = float(re.sub(',', '', row[3]))
                    info['value'] = float(re.sub(',', '', row[6]))
                except ValueError:
                    info['quantity'] = 0
                    info['value'] = 0
                except:
                    raise

                name = row[1].strip()
                info['name_id'] = fetch.sql_insert_one_get_id(
                    cur, 'Insiders', 'name', name)

                type = row[5].strip()
                if ' ' in type:
                    type = type.split()[0]
                info['transaction_id'] = fetch.sql_insert_one_get_id(
                    cur, 'TransactionType', 'type', type)

                # Insert data into tables
                info['ticker_id'] = ticker_id
                info['exchange_id'] = exch_id
                sql = fetch.sql_insert('InsiderTransactions',
                    tuple(info.keys()), tuple(info.values()))
                fetch.db_execute(cur, sql)

    return 200


# https://finance.yahoo.com/quote/
def parse_10(cur, ticker_id, exch_id, data):

    sql = 'SELECT ticker FROM Tickers WHERE id = ?'
    ticker  = db_execute_tpl(cur, sql, (ticker_id,)).fetchall()[0][0]

    soup = bs(data, 'html.parser')

    print()
    print(ticker)
    print(soup.prettify())
    # for table in tables:
    #     print()
    #     print(table)
    exit()
    # <table class="W(100%)" data-reactid="35">
    # <table class="W(100%) M(0) Bdcl(c)" data-reactid="76">


# Generate UPDATE SQL command
def update_record(table, dict1, dict2):
    updates = str(dict1).replace('{\'', '').replace(', \'', ', ')
    updates = updates.replace('}', '').replace('\':', ' =')
    conds = str(dict2).replace('{\'', '(')
    conds = conds.replace('}', ')').replace('\':', ' =')
    conds = conds.replace(', \'', ' AND ')
    sql = 'UPDATE OR IGNORE ' + table + ' SET ' + updates + ' WHERE ' + conds
    sql = re.sub('\'null\'', 'null', sql)
    return sql
