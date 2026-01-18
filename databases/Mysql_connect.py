from mysql.connector import Error
import mysql.connector

class mysql_connection:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password
        }
        self.connection = None
        self.cursor = None
    def mysql_connect(self):
        try:
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor()
            print('--------Mysql is successfully connected---------')
            return self.connection, self.cursor
        except Error as e:
            raise Exception(f'Mysql meet error in {e}') from e
    def mysql_close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
        print('--------Mysql is completed connect---------')
    def __enter__(self):
        self.mysql_connect()
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.mysql_close()

