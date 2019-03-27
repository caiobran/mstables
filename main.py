#!/usr/bin/env python

from shutil import copyfile
from datetime import datetime
from importlib import reload
import fetch, time, os, re, sqlite3

__author__ = "Caio Brandao"
__copyright__ = "Copyright 2019+, Caio Brandao"
__license__ = "MIT"
__version__ = "0.0"
__maintainer__ = "Caio Brandao"
__email__ = "caiobran88@gmail.com"


# Create back-up file under /db/backup
def backup_db(file):
    #today = datetime.today().strftime('%Y%m%d%H')
    new_file = db_file['db_backup'].format(
        input('Enter back-up file name:\n'))
    fetch.print_('Please wait ... Database file is being backed-up ...')
    copyfile(db_file['path'], new_file)
    return '\n~ Back-up file saved\t{}'.format(new_file)


# Change variable for .sqlite file name based on user input
def change_name(old_name):
    msg = 'Existing database files in directory \'db/\': {}\n'
    msg += 'Enter new name for .sqlite file (current = \'{}\'):\n'
    fname = lambda x: re.sub('.sqlite', '', x)
    files = [fname(f) for f in os.listdir('db/') if '.sqlite' in f]
    return input(msg.format(files, old_name))


# Print options menu
def print_menu(names):
    gap = 22
    dash = '='
    banner = ' Welcome to msTables '
    file = '\'{}.sqlite\''.format(db_file['name'])
    menu = {
        '0' : 'Change database file name (current name = {})'.format(file),
        '1' : 'Create database tables',
        '2' : 'Download data from MorningStar.com into database',
        '3' : 'Erase all records from database tables',
        '4' : 'Delete all database tables',
        '5' : 'Erase all downloaded history from \'Fetched_urls\' table',
        #'X' : 'Parse (FOR TESTING PURPOSES)',
        '6' : 'Create a database back-up file'
    }

    print(dash * (len(banner) + gap * 2))
    print('{}{}{}'.format(dash * gap, banner, dash * gap))
    print('\nAvailable actions:\n')
    for k, v in menu.items():
        print(k, '-', v)
    print('\n' + dash * (len(banner) + gap * 2))

    return menu


# Print command line menu for user input
def main(file):
    while True:

        # Print menu and capture user selection
        ops = print_menu(file)
        while True:
            try:
                inp0 = input('Enter action no.: ').strip()
                break
            except KeyboardInterrupt:
                print('\nGoodbye!')
                exit()
        if inp0 not in ops.keys(): break
        reload(fetch) #Comment out after development
        start = time.time()
        inp = int(inp0)

        # Call function according to user input
        msg = '\nAre you sure you would like to {}? (Y/n):\n'
        if input(msg.format(ops[inp0].lower())).lower() == 'y':
            print()
            try:
                # Change db file name
                if inp == 0:
                    db_file['name'] = change_name(db_file['name'])
                    start = time.time()
                    db_file['path'] = db_file['npath'].format(db_file['name'])

                # Create database tables
                elif inp == 1:
                    msg = fetch.create_tables(db_file['path'])

                # Download data from urls listed in api.json
                elif inp == 2:
                    start = fetch.fetch(db_file['path'])
                    msg = '\n~ Database updated successful.'

                # Erase records from all tables
                elif inp == 3:
                    msg = fetch.erase_tables(db_file['path'])

                # Delete all tables
                elif inp == 4:
                    msg = fetch.delete_tables(db_file['path'])

                # Delete Fetched_urls table records
                elif inp == 5:
                    msg = fetch.delfetchhis(db_file['path'])

                # Back-up database file
                elif inp == int(list(ops.keys())[-1]):
                    msg = backup_db(db_file)

                # TESTING
                elif inp == 99:
                    fetch.parse.parse(db_file['path'])
                    msg = 'FINISHED'

            except sqlite3.OperationalError as S:
                msg = '### Error message - {}'.format(S) + \
                    '\n### Scroll up for more details. If table does not ' + \
                    'exist, make sure to execute action 1 before choosing' + \
                    ' other actions.'
                pass
            except KeyboardInterrupt:
                print('\nGoodbye!')
                exit()
            except Exception as e:
                print('\n\n### Error @ main.py:\n {}\n'.format(e))
                raise e

            # Print output message
            os.system('clear')
            print(msg)

            # Calculate and print execution time
            end = time.time()
            print('\n~ Execution Time\t{:.2f} sec\n'.format(end - start))
        else:
            os.system('clear')

# Define database (db) file and menu text variables
db_file = dict()
db_file['npath'] = 'db/{}.sqlite'
db_file['name'] = 'mstables'
db_file['path'] = db_file['npath'].format(db_file['name'])
db_file['db_backup'] = 'db/backup/{}.sqlite'

if __name__ == '__main__':
    os.system('clear')
    main(db_file)
    print('Goodbye!\n\n')
