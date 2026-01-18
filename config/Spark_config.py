from pyspark.sql import SparkSession
from typing import Dict, List, Optional
from config.Database_config import set_database_config

class Set_SparkSession:
    def __init__(self,
                 app_name: str = None,
                 master_url: str = None,
                 executor_cores: Optional[int] = None,
                 executor_memory: Optional[str] = None,
                 driver_memory: Optional[str] = None,
                 executor_nums: Optional[int] = None,
                 jar_packages: Optional[List[str]] = None,
                 spark_conf: Optional[Dict[str, str]] = None,
                 log_level: str = None
                 ):
        self.spark = self.set_sparksession(
            app_name,
            master_url,
            executor_cores,
            executor_memory,
            driver_memory,
            executor_nums,
            jar_packages,
            spark_conf,
            log_level
        )
    def set_sparksession(self,
                         app_name: str = 'SparkSession',
                         master_url: str = 'local[*]',
                         executor_cores: Optional[int] = 3,
                         executor_memory: Optional[str] = '2g',
                         driver_memory: Optional[str] = '1g',
                         executor_nums: Optional[int] = 1,
                         jar_packages: Optional[List[str]] = None,
                         spark_conf: Optional[Dict[str, str]] = None,
                         log_level: str = 'ERROR'
                         ) -> SparkSession:
        builder = SparkSession.builder \
        .appName(app_name) \
        .master(master_url)
        if executor_cores:
            builder.config('spark.executor.cores', executor_cores)
        if executor_memory:
            builder.config('spark.executor.memory', executor_memory)
        if driver_memory:
            builder.config('spark.driver.memory', driver_memory)
        if executor_nums:
            builder.config('spark.executor.instances', executor_nums)
        if jar_packages:
            set_jar_packages = ','.join([jar_package for jar_package in jar_packages])
            builder.config('spark.jars.packages', set_jar_packages)
        if spark_conf:
            for key, value in spark_conf.items():
                builder.config(key, value)
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)
        return spark
def set_spark_config():
    db_config = set_database_config()
    spark_config = {
        'mysql': {
            'jdbc': 'jdbc:mysql://{}:{}/{}'.format(db_config['mysql'].host, db_config['mysql'].port, db_config['mysql'].database),
            'host': db_config['mysql'].host,
            'port': db_config['mysql'].port,
            'user': db_config['mysql'].user,
            'password': db_config['mysql'].password,
            'database': db_config['mysql'].database
        },
        'mongodb': {
            'uri': db_config['mongodb'].url,
            'database': db_config['mongodb'].database
        }
    }
    return spark_config