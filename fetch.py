import xml.etree.ElementTree as ET
import multiprocessing as mp
from datetime import datetime
from importlib import reload #Comment out once done using
from csv import reader
import numpy as np
import pandas as pd
import requests, sqlite3, time, json, zlib, re, os, parse


def create_tables(db_file):

    def mssitemap():
        tree = ET.parse('input/ms_sal-quote-stock-sitemap.xml')
        url_tag = '{http://www.sitemaps.org/schemas/sitemap/0.9}url'
        loc_tag = '{http://www.sitemaps.org/schemas/sitemap/0.9}loc'
        urls = tree.findall('{}/{}'.format(url_tag, loc_tag))
        get_ticker = lambda st: re.findall('/stocks/(\S+)/',
            st)[0].split('/')[1].upper()
        return [(get_ticker(url.text),) for url in urls]


    # Create database connection
    print_('\nPlease wait, database tables are being created ...')
    conn = sqlite3.connect(db_file)
    conn.execute('pragma auto_vacuum = 1')
    cur = conn.cursor()

    # Create database tables based on table.json
    for table in tbl_names:
        columns = ' '.join(['{} {}'.format(k, v) for k, v in tbl_js[table].items()])
        sql = 'CREATE TABLE IF NOT EXISTS {} ({})'.format(table, columns)
        db_execute(cur, sql)

    # Insert list of tickers and exchanges previously retrieved into database
    file = 'ticker_exch.json'
    if file in os.listdir(fd_input):
        with open(fd_input + file) as fi:
            tbls = json.load(fi)
        for tbl in tbls:
            if tbl == 'Tickers':
                col = '(id, ticker)'
                val = '(?, ?)'
            elif tbl == 'Exchanges':
                col = '(id, exchange, exchange_sym, country_id)'
                val = '(?, ?, ?, ?)'
            elif tbl == 'Master':
                col = '(ticker_id, exchange_id)'
                val = '(?, ?)'
            sql = 'INSERT OR IGNORE INTO {} {} VALUES {}'.format(tbl, col, val)
            cur.executemany(sql, tbls[tbl])
    else:
        std_list = [
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
            'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'
        ]
        sql = 'INSERT OR IGNORE INTO tickers (ticker) VALUES (?)'
        cur.executemany(sql, mssitemap() + std_list)

    # Insert list of countries into Countries table
    sql = '''INSERT OR IGNORE INTO Countries
        (country, a2_iso, a3_un) VALUES (?, ?, ?)'''
    cur.executemany(sql, csv_content('input/ctycodes.csv', 3))

    # Insert list of currencies into Currencies table
    sql = '''INSERT OR IGNORE INTO Currencies (currency, code) VALUES (?, ?)'''
    cur.executemany(sql, csv_content('input/symbols.csv', 2))

    # Insert list of types into SecurityTypes table
    sql = '''INSERT OR IGNORE INTO SecurityTypes
        (security_type_code, security_type) VALUES (?, ?)'''
    cur.executemany(sql, csv_content('input/ms_investment-types.csv', 2))

    # Insert list of api URLs into URLs table
    for k, v in apis.items():
        sql = sql_insert('URLs', '(id, url)', (k, v))
        db_execute(cur, sql)

    save_db(conn)
    cur.close()
    conn.close()

    return '\n\n~ Database tables created.'


def csv_content(file, columns, header=False):
    with open(file) as csvfile:
        info = reader(csvfile)#, delimiter=',', quotechar='"')
        if header == True:
            return [row[:columns] for row in info]
        return [row[:columns] for row in info][1:]


def delete_tables(db_file):
    print_('\nPlease wait, database tables are being deleted ...')

    # Create database connection
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()

    # Drop tables and commit database
    for table in tbl_names:
        print_('Deleting database table {} ...'.format(table))
        sql = 'DROP TABLE IF EXISTS ' + table
        db_execute(cur, sql)
    save_db(conn)

    cur.close()
    conn.close()

    return '\n\n~ Database tables deleted.'


def delfetchhis(db_file):
    print_('\nPlease wait, download history is being erased ...')

    # Create database connection
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()

    # Drop tables and commit database
    table = 'Fetched_urls'
    sql = 'DELETE FROM ' + table
    db_execute(cur, sql)
    save_db(conn)

    cur.close()
    conn.close()

    return '\n\n~ Download history (table Fetched_urls) erased.'


def erase_tables(db_file):
    print_('\nPlease wait, database tables are being erased ...')

    # Create database connection
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    for table in tbl_names:
        print_('Erasing database table {} ...'.format(table))
        sql = 'DELETE FROM ' + table
        db_execute(cur, sql)
    save_db(conn)
    cur.close()
    conn.close()

    return '\n\n~ Database tablea erased.'


def db_execute(cur, sql):
    x = 0
    while x < 10:
        try:
            return cur.execute(sql)
        except KeyboardInterrupt:
            print('\nGoodbye!')
            exit()
        except Exception as e:
            if x == 9:
                print('\n\nSQL cmd = \'{}\'\n'.format(sql))
                print(type(e))
                print(dir(e))
                raise
        x += 1


def fetch(db_file):
    div = 120
    pool_size = 30

    # Get user input for stp (no. of tickers to update)
    while True:
        # User input for number of tickers to update
        try:
            msg = 'Qty. of records to be updated:\n:'
            stp = int(input(msg))
        except KeyboardInterrupt:
            print('\nGoodbye!')
            exit()
        except Exception:
            continue
        start = time.time()
        break

    # Fetch data for each API for tickers qty. = 'stp'
    dividend = max(stp, div)
    runs = dividend // div
    div = min(stp, div)
    tickers = []
    for i in range(runs):
        t0 = time.time()

        # Create db connection
        conn = sqlite3.connect(db_file)
        cur = conn.cursor()

        # Get list of URL's to be retrieved and print current run info
        msg = '\nRun {} / {}'
        if i == 0:
            try:
                urls = geturllist(cur)
            except KeyboardInterrupt:
                print('\nGoodbye!')
                exit()
            except:
                raise
            msg0 = '\nTotal URL requests pending =\t{:9,.0f}\n'
            msg0 += 'Total URL requests planned =\t{:9,.0f}\n'
            print(msg0.format(len(urls), stp * len(apis)))
            msg0 = '\t({} requests per API per run = {} requests per run)'
            msg += msg0.format(div, min(stp, div)*len(apis))
        print(msg.format(i+1, runs))

        j = i * div * len(apis)
        items = urls[j:j + div * len(apis)]
        sort0 = lambda x: (x[0], x[2], x[3])
        items = sorted(items, key=sort0)

        # Fetch data from API's using multiprocessing.Pool
        results = []
        while True:
            try:
                with mp.Pool(pool_size) as p:
                    #r = p.imap_unordered(fetch_api, items)
                    #r = p.map(fetch_api, items)
                    r = p.imap(fetch_api, items)
                    for turn in range(len(items)):
                        try:
                            results.append(r.next(timeout=5))
                        except mp.context.TimeoutError:
                            pass
                        '''if turn % pool_size == 0:
                            time.sleep(0.5)'''
                break
            except KeyboardInterrupt:
                print('\nGoodbye!')
                exit()
            except:
                raise

        '''for item in url_info:
            results.append(fetch_api(item))'''

        # Enter URL data into Fetched_urls
        if results != []:
            results = list(filter(lambda x: x is not None, results))
            msg = '\t- Successful requests:\t{:,.0f} out of {:,.0f} ({:.1%})'
            totreq = min(stp, div)*len(apis)
            srate = len(results)/totreq
            print_('')
            print(msg.format(len(results), totreq, srate))

            # Insert new data
            msg = 'Storing source data into database table \'Fetched_urls\'...'
            print_(msg)
            cols = 'url_id, ticker_id, exch_id, fetch_date, ' + \
                'status_code, source_text'
            sql = 'INSERT OR IGNORE INTO Fetched_urls ({}) VALUES ({})'
            sql = sql.format(cols, '?, ?, ?, date(?), ?, ?')
            #print('\n\nSQL = {}'.format(sql))
            cur.executemany(sql, results)

        # Export new ticker and exchange lists to input folder
        output = {}
        with open(fd_input + 'ticker_exch.json', 'w') as file:
            sql = 'SELECT * FROM Tickers'
            ticks = cur.execute(sql).fetchall()
            output['Tickers'] = ticks
            sql = 'SELECT * FROM Exchanges'
            exchs = cur.execute(sql).fetchall()
            output['Exchanges'] = exchs
            sql = 'SELECT ticker_id, exchange_id FROM Master'
            fetched = cur.execute(sql).fetchall()
            output['Master'] = fetched
            file.write(json.dumps(output, indent=2))

        # Save (commit) changes and close db
        save_db(conn)
        cur.close()
        conn.close()

        # Call parsing module from parse.py
        parse.parse(db_file)
        t1 = time.time()
        print_('\t- Run {} duration:\t{:.2f} sec'.format(i+1, t1-t0))

    return start


def fetch_api(url_info):
    t0 = time.time()

    # Unpack variables
    url_id, url, ticker_id, exch_id = url_info
    num = ticker_list[url_id]['{}:{}'.format(exch_id, ticker_id)]
    ct = ticker_count[url_id]

    # Fetch URL data
    x = 0
    while True:
        try:
            page = requests.get(url)
            status_code = page.status_code
            data = re.sub('\'', '', page.text)
            data = zlib.compress(data.encode())
            break
        except requests.exceptions.ConnectionError:
            if x > 9:
                print_('')
                print('\n\tError: requests.exceptions.ConnectionError')
                msg = 'Ticker: {}, Exch: {}, URL: {}\n'
                print(msg.format(ticker_id, exch_id, url))
                return
        except requests.exceptions.ChunkedEncodingError:
            print_('')
            print('\n\tError: requests.exceptions.ChunkedEncodingError')
            msg = 'Ticker: {}, Exch: {}, URL: {}\n'
            print(msg.format(ticker_id, exch_id, url))
            time.sleep(4)
            return
        except KeyboardInterrupt:
            print('\nGoodbye!')
            exit()
        except:
            raise
        x += 1

    # Timer to attemp to slow down and 'align' Pool requests to every sec
    if True:
        time.sleep(1 - (time.time() % 1))
    printprogress(url_id, num, ct)
    #time.sleep(sec)

    return (url_id, ticker_id, exch_id, today, status_code, data)


def geturllist(cur):

    urls = []
    api = [(int(k), v) for k, v in apis.items()]

    for url_id, url0 in api:

            # Select list of tickers not yet updated for current API
            print_('Creating URL list for API {} ...'.format(url_id))
            with open(sql_cmds.format('select_notupdated1.txt')) as file:
                sql_cmd1 = file.read().strip()
            with open(sql_cmds.format('select_notupdated2.txt')) as file:
                sql_cmd2 = file.read().strip()
            if url_id in [1, 2, 3]:
                sql = sql_cmd1.format(url_id)
            else:
                sql = sql_cmd2.format(url_id)
            tickers = db_execute(cur, sql).fetchall()
            ticker_count[url_id] = len(tickers)
            ticker_list[url_id] = {}
            #print('\n\n*** SQL = {}\n'.format(sql))

            # Create list of URL's for each ticker
            def url_list(ct, tick):
                exch_id, exch_sym = tick[0], tick[1]
                sym_id, symbol = tick[2], tick[3]
                url = url0.format(exch_sym, symbol)
                ticker_list[url_id]['{}:{}'.format(exch_id, sym_id)] = ct
                return (url_id, url, sym_id, exch_id)

            urls = urls + [url_list(c, ticker)
                for c, ticker in enumerate(tickers)]

    # Print API list and no. of tickers to be updated for each
    msg = '\nQty. of records pending update per API no.:\n\n'
    print_(msg)
    df_tickct = pd.DataFrame([(k, '{:8,.0f}'.format(v))
        for k, v in ticker_count.items()])
    print(df_tickct.rename(columns={0:'API', 1:'Pending'})
            .set_index('API'))
    df_tickct = None
    return sorted(urls, key=lambda x: (x[2], x[3], x[0]))


def print_(msg):
    msg = 'echo -en "\\r\\e[K{}"'.format(msg)
    os.system(msg)


def printprogress(api, num, ct):
    msg = 'Fetching API {:.0f}... {:7,.0f} / {:7,.0f}  ({:.2%})'
    msg = msg.format(api, num+1, ct, (num+1)/ct)
    msg = 'echo -en "\\r\\e[K{}"'.format(msg)
    os.system(msg)


def save_db(conn):
    err = None
    while True:
        try:
            conn.commit()
        except sqlite3.OperationalError as err1:
            if err != err1:
                err = err1
                print(err)
            continue
        except Exception as errs:
            print(errs)
            raise
        break


def sql_insert(table, columns, values):
    if len(values) == 1:
        values = '(\'{}\')'.format(values[0])

    sql = 'INSERT OR IGNORE INTO {} {} VALUES {}'
    sql = sql.format(table, columns, values)
    return sql


def sql_record_id(table, column, value):
    if type(value) is str:
        sql = 'SELECT id FROM {} WHERE {} =\'{}\''
    else:
        sql = 'SELECT id FROM {} WHERE {} ={}'
    return sql.format(table, column, value)


def sql_insert_one_get_id(cur, tbl, col, val):

    # Insert value into db table
    column = '({})'.format(col)
    sql1 = sql_insert(tbl, column, (val,))
    sql2 = sql_record_id(tbl, column, val)

    # Select ID from table for value
    try:
        db_execute(cur, sql1)
        id = db_execute(cur, sql2).fetchone()[0]
    except:
        print('\n\n\t# Error @ SQL1 =', sql1, '\n\nSQL2 =', sql2, '\n\n')
        raise

    return id


# Reference variables
reload(parse) #Comment out once done using
ticker_list = {}
ticker_count = {}
fd_input = 'input/'
today   = datetime.today().strftime('%Y-%m-%d')
sql_cmds = '{}sql_cmd/{}'.format(fd_input, '{}')
with open('{}/api.json'.format(fd_input)) as file:
    apis = json.load(file)
with open('{}/tables.json'.format(fd_input)) as file:
    tbl_js = json.load(file)
tbl_names = list(tbl_js.keys())
