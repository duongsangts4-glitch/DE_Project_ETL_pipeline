from config.Database_config import set_database_config
from databases.Mongodb_connect import Mongodb_connection
from databases.Mysql_connect import mysql_connection
from databases.Create_schema_database import schema_mysql, schema_mongodb

from config.Spark_config import set_spark_config
from src.spark.Spark_write_mysql import SparkWriteToMysql
from src.spark.Spark_write_mongodb import SparkWriteToMongodb
from src.spark.Spark_transformation import test_spark, run_etl_process

def main(config):
    conf = config['mysql']
    with mysql_connection(conf.host, conf.port, conf.user, conf.password) as mysqlconnect:
        connection, cursor = mysqlconnect.connection, mysqlconnect.cursor
        schema_mysql(connection, cursor)

    conf1 = config['mongodb']
    with Mongodb_connection(conf1.url, conf1.database) as mongoconnect:
        db = mongoconnect.mongodb_connect()
        schema_mongodb(db)
    run_etl_process()
if __name__ == '__main__':
    config = set_database_config()
    main(config)