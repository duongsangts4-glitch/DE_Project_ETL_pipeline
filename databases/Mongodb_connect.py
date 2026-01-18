from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

class Mongodb_connection:
    def __init__(self, url, database):
        self.url = url
        self.database = database
        self.connect = None
        self.db = None
    def mongodb_connect(self):
        try:
            self.connect = MongoClient(self.url)
            self.connect.admin.command('ping')
            self.db = self.connect[self.database]
            print('--------Mongodb is successfully connected---------')
            return self.db
        except ConnectionFailure as c:
            raise Exception(f'Mongodb meet error in {c}') from c
    def mongodb_close(self):
        if self.connect:
            self.connect.close()
        print('--------Mongodb is completed connected---------')
    def __enter__(self):
        self.mongodb_connect()
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.mongodb_close()