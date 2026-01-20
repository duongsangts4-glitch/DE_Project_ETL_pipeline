from mysql.connector import Error
def schema_mysql(connection, cursor):
    database = 'github_data'
    cursor.execute(f'DROP DATABASE IF EXISTS {database}')
    print(f'--------Database {database} is just dropped---------')
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {database}')
    print(f'--------Database {database} is just created---------')
    connection.database = database
    connection.commit()
    try:
        with open('../src/schema_sql/schema_new.sql', 'r') as files:
            file = files.read()
            command_sql = [cmd.strip() for cmd in file.split(';') if cmd.strip()]
            for cmd in command_sql:
                cursor.execute(cmd)
            print(f'--------Schema of database {database} is created-----------')
    except Error as e:
        connection.callback()
        raise Exception(f'Have error while create schema {e}') from e
def schema_mongodb(db):
    collections_info = {
        "users": {
            "bsonType": "object",
            "required": ["user_id", "login"],
            "properties": {
                "user_id": {"bsonType": "long"},
                "login": {"bsonType": "string"},
                "avatar_url": {"bsonType": "string"},
                "url": {"bsonType": "string"},
                "html_url": {"bsonType": "string"},
                "type": {"bsonType": "string"},
                "site_admin": {"bsonType": "bool"}
            }
        },
        "repositories": {
            "bsonType": "object",
            "required": ["repo_id", "name"],
            "properties": {
                "repo_id": {"bsonType": "long"},
                "name": {"bsonType": "string"},
                "url": {"bsonType": "string"}
            }
        },
        "events": {
            "bsonType": "object",
            "required": ["event_id", "actor_id", "repo_id"],
            "properties": {
                "event_id": {"bsonType": "string"},
                "event_type": {"bsonType": "string"},
                "public": {"bsonType": "bool"},
                "created_at": {"bsonType": "date"},
                "actor_id": {"bsonType": "long"},
                "repo_id": {"bsonType": "long"}
            }
        },
        "issues": {
            "bsonType": "object",
            "required": ["issue_id", "repo_id", "user_id"],
            "properties": {
                "issue_id": {"bsonType": "long"},
                "repo_id": {"bsonType": "long"},
                "number": {"bsonType": "int"},
                "title": {"bsonType": "string"},
                "state": {"bsonType": "string"},
                "locked": {"bsonType": "bool"},
                "comments_count": {"bsonType": "int"},
                "created_at": {"bsonType": "date"},
                "updated_at": {"bsonType": "date"},
                "closed_at": {"bsonType": ["date", "null"]},
                "body": {"bsonType": "string"},
                "user_id": {"bsonType": "long"}
            }
        },
        "labels": {
            "bsonType": "object",
            "required": ["name"],
            "properties": {
                "label_id": {"bsonType": "long"},
                "name": {"bsonType": "string"},
                "color": {"bsonType": "string"},
                "url": {"bsonType": "string"}
            }
        },
        "issue_labels": {
            "bsonType": "object",
            "required": ["issue_id", "label_id"],
            "properties": {
                "issue_id": {"bsonType": "long"},
                "label_id": {"bsonType": "long"}
            }
        },
        "comments": {
            "bsonType": "object",
            "required": ["comment_id", "issue_id", "user_id"],
            "properties": {
                "comment_id": {"bsonType": "long"},
                "issue_id": {"bsonType": "long"},
                "user_id": {"bsonType": "long"},
                "url": {"bsonType": "string"},
                "html_url": {"bsonType": "string"},
                "created_at": {"bsonType": "date"},
                "updated_at": {"bsonType": "date"},
                "body": {"bsonType": "string"}
            }
        },
        "payloads": {
            "bsonType": "object",
            "required": ["event_id"],
            "properties": {
                "event_id": {"bsonType": "string"},
                "action": {"bsonType": "string"}
            }
        }
    }
    for name, schema in collections_info.items():
        db.drop_collection(name)
        print(f'--------Mongodb is just drop collection {name}--------')
        db.create_collection(name, validator={'jsonSchema': schema})
    print(f'---------Mongodb is create schema successfully {name}---------')