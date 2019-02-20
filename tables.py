import json

class tables():

    def __init__(self, json_file):
        with open(json_file) as file:
            self.js = json.load(file)
        self.table_names = list(self.js.keys())

    def get_columns(self, table):
        cols = self.js[table]
        return ' '.join(['{} {}'.format(k, v) for k, v in cols.items()])
