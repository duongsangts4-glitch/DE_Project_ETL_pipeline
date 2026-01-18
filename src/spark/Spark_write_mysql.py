from pyspark.sql import SparkSession, DataFrame
from typing import Dict

class SparkWriteToMysql:
    def __init__(self, spark: SparkSession, mysql_config: Dict):
        self.spark = spark
        self.mysql_config = mysql_config
    def sparkwritetomysql(self, df: DataFrame, user: str, password: str, jdbc: str, mysql_table: str, mode: str):
            df.write.format("jdbc") \
            .option("url", jdbc) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", mysql_table) \
            .option("user", user) \
            .option("password", password) \
            .mode(mode) \
            .save()
            print(f'Successfully wrote to Mysql table {mysql_table}')
