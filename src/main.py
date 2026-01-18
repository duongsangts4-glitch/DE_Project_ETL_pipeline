from config.Database_config import set_database_config
from databases.Mongodb_connect import Mongodb_connection
from databases.Mysql_connect import mysql_connection
from databases.Create_schema_database import schema_mysql, schema_mongodb
from config.Spark_config import set_spark_config
from src.spark.Spark_write_mysql import SparkWriteToMysql
from src.spark.Spark_write_mongodb import SparkWriteToMongodb
from src.spark.Spark_transformation import test_spark, dataframe_transform

def main(config,db_config, ):
    conf = config['mysql']
    with mysql_connection(conf.host, conf.port, conf.user, conf.password) as mysqlconnect:
        connection, cursor = mysqlconnect.connection, mysqlconnect.cursor
        schema_mysql(connection, cursor)

    conf1 = config['mongodb']
    with Mongodb_connection(conf1.url, conf1.database) as mongoconnect:
        db = mongoconnect.mongodb_connect()
        schema_mongodb(db)



    mysql_config = db_config['mysql']
    mongodb_config = db_config['mongodb']
    SparkWriteToMysql(test_spark().spark, mysql_config) \
        .sparkwritetomysql(dataframe_transform(), mysql_config['user'], mysql_config['password'], mysql_config['jdbc'], 'Demo',
                           'append')
    SparkWriteToMongodb(test_spark().spark, mongodb_config) \
        .sparkwritetomongodb(dataframe_transform(), mongodb_config['uri'], mongodb_config['database'], 'Demo', 'append')

    # dataframe_transform()
if __name__ == '__main__':
    config = set_database_config()
    db_config = set_spark_config()
    main(config,db_config)