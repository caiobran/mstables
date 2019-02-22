import json

class tables():

    def __init__(self, json_file):
        with open(json_file) as file:
            self.js = json.load(file)
        self.names = list(self.js.keys())
