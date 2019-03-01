#!/usr/bin/env python

__author__ = "Caio Brandao"
__copyright__ = "Copyright 2019+, Caio Brandao"
__license__ = "GPLv3"
__version__ = "1.0"
__maintainer__ = "Andrea Lazzarotto"
__email__ = "andrea.lazzarotto@gmail.com"

from shutil import copyfile
from datetime import datetime
from importlib import reload
import update as up
import time, os, re


# Create back-up file under /db/backup
def backup_db(curr_file):
    today = datetime.today().strftime('%Y%m%d%H')
    new_file = db_backup#.format(today)
    up.print_('Please wait ... Database file is being backed-up ...')
    copyfile(curr_file, new_file)


# Change variable for .sqlite file name based on user input
def change_name(old_name):
    msg = 'Existing database files in directory \'db/\': {}\n'
    msg += 'Enter new name for .sqlite file (current = \'{}\'):\n:'
    fname = lambda x: re.sub('.sqlite', '', x)
    files = [fname(f) for f in os.listdir('db/') if '.sqlite' in f]
    return input(msg.format(files, old_name))


# Define database (db) file and menu text variables
file = 'equitable'
db = 'db/{}.sqlite'
db_file = db.format(file)
db_backup = 'db/backup/backup.sqlite'#_{}.sqlite'
banner = ' Welcome to equiTable '
gap = 6
dash = '-'

# Clear terminal, print menu and get user input
os.system('clear')
while True:

    # Print options menu
    options = {
        '0' : 'Change file name (current = \'{}.sqlite\')'.format(file),
        '1' : 'Create tables',
        '2' : 'Erase table records',
        '3' : 'Delete tables',
        '4' : 'Fetch data from API\'s',
        '5' : 'Create database backup'
    }
    print(dash * (len(banner) + gap * 2))
    print('{}{}{}'.format(dash * gap, banner, dash * gap))
    print('Menu:\n')
    for k, v in options.items():
        print(k, dash, v)
    print('\n' + dash * (len(banner) + gap * 2))

    # Capture user selection and clear terminal output
    inp0 = input('Enter option no.:\n:').strip()
    if inp0 not in options.keys():
        break
    print()

    reload(up)
    inp = int(inp0)
    start = time.time()
    os.system('clear')


    # Call function according to user input
    try:
        # Change db file name
        if inp == 0:
            file = change_name(file)
            db_file = db.format(file)
        # Create database tables
        elif inp == 1:
            up.create_tables(db_file)
        # Erase records from all tables
        elif inp == 2:
            up.erase_tables(db_file)
        # Delete all tables
        elif inp == 3:
            up.delete_tables(db_file)
        # Back-up database file
        elif inp == int(list(options.keys())[-1]):
            backup_db(db_file)

    except Exception as e:
        print('# ERROR:', e)

    # Call Fetch function to download data from urls listed in api.json
    if inp == 4:
        start = up.fetch(db_file)

    end = time.time()
    msg = '\n~ Execution Time\t{:.3f} sec\n\n'.format(end - start)
    up.print_(msg)
