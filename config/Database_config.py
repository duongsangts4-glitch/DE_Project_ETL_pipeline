from dotenv import load_dotenv
import os
from dataclasses import dataclass


@dataclass
class mysql_type():
    host : str
    port : int
    user : str
    password : str
    database : str

@dataclass
class mongodb_type():
    url : str
    database : str

def set_database_config():
    load_dotenv()
    config = {
        'mysql': mysql_type(
            host = os.getenv('MYSQL_HOST'),
            port = os.getenv('MYSQL_PORT'),
            user = os.getenv('MYSQL_USER'),
            password = os.getenv('MYSQL_PASSWORD'),
            database = os.getenv('MYSQL_DATABASE'))
        ,
        'mongodb': mongodb_type(
            url = os.getenv('MONGODB_URL'),
            database = os.getenv('MONGODB_DB')
        )

    }
    return config