import tables
import sqlite3
import os

'''for file in os.listdir('db'):
    print(file)'''

db_file = 'db/equitable.sqlite'
tbl_file = 'input/tables.json'

conn = sqlite3.connect(db_file)
cur = conn.cursor()
tbls = tables.tables(tbl_file)

for i, tbl in enumerate(tbls.names):
    print(i, tbl)

cur.close()
conn.close()
