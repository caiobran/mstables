#!/usr/bin/env python

from shutil import copyfile
from datetime import datetime
from importlib import reload
import update as up
import time, os, re

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
        input('Enter back-up file name:\n:'))
    up.print_('Please wait ... Database file is being backed-up ...')
    copyfile(db_file['path'], new_file)
    return '\n~ Back-up file saved\t{}'.format(new_file)


# Change variable for .sqlite file name based on user input
def change_name(old_name):
    msg = 'Existing database files in directory \'db/\': {}\n'
    msg += 'Enter new name for .sqlite file (current = \'{}\'):\n:'
    fname = lambda x: re.sub('.sqlite', '', x)
    files = [fname(f) for f in os.listdir('db/') if '.sqlite' in f]
    return input(msg.format(files, old_name))


# Print options menu
def print_menu(names):
    gap = 6
    dash = '-'
    banner = ' Welcome to equiTable '
    file = '\'{}.sqlite\''.format(db_file['name'])
    menu = {
        '0' : 'Change file name (current = {})'.format(file),
        '1' : 'Create tables',
        '2' : 'Erase table records',
        '3' : 'Delete tables',
        '4' : 'Fetch data from API\'s',
        '5' : 'Create database backup'
    }

    print(dash * (len(banner) + gap * 2))
    print('{}{}{}'.format(dash * gap, banner, dash * gap))
    print('Menu:\n')
    for k, v in menu.items():
        print(k, dash, v)
    print('\n' + dash * (len(banner) + gap * 2))

    return menu


# Print command line menu for user input
def main(file):
    while True:

        # Print menu and capture user selection
        options = print_menu(file)

        inp0 = input('Enter option no.:\n:').strip()
        if inp0 not in options.keys():
            break
        start = time.time()
        inp = int(inp0)
        msg = ''
        reload(up)
        print()

        # Call function according to user input
        try:
            # Change db file name
            if inp == 0:
                db_file['name'] = change_name(db_file['name'])
                db_file['path'] = db_file['npath'].format(db_file['name'])

            # Create database tables
            elif inp == 1:
                msg = up.create_tables(db_file['path'])

            # Erase records from all tables
            elif inp == 2:
                msg = up.erase_tables(db_file['path'])

            # Delete all tables
            elif inp == 3:
                msg = up.delete_tables(db_file['path'])

            # Back-up database file
            elif inp == int(list(options.keys())[-1]):
                msg = backup_db(db_file)

        except Exception as e:
            print('\n\n### ERROR @ Main.py:\n', e, '\n')
            raise e

        # Call Fetch function to download data from urls listed in api.json
        if inp == 4:
            start = up.fetch(db_file['path'])

        end = time.time()
        os.system('clear')
        print(msg)
        msg = '\n~ Execution Time\t{:.3f} sec\n'.format(end - start)
        print(msg)


# Define database (db) file and menu text variables
db_file = dict()
db_file['npath'] = 'db/{}.sqlite'
db_file['name'] = 'equitable'
db_file['path'] = db_file['npath'].format(db_file['name'])
db_file['db_backup'] = 'db/backup/{}.sqlite'

if __name__ == '__main__':
    os.system('clear')
    main(db_file)
    print('Goodbye!\n\n')
