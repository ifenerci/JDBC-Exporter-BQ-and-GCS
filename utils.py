import json
from pyspark.sql import SparkSession

def read_config(config_file):
    with open(config_file) as f:
        config = json.load(f)
    f.close()

    return config

def get_spark_session(jdbc_driver_path, gcs_con_path,bq_con_path, gcp_credentials_file):

    spark_jars = '{},{},{}'.format(
        jdbc_driver_path,
        gcs_con_path,
        bq_con_path
    )

    spark = SparkSession.builder \
        .config('spark.jars', spark_jars) \
        .appName('JDBCExporter') \
        .getOrCreate()

    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile',
                                         gcp_credentials_file)
    return spark

def read_data(spark, jdbc_url, user, password, driver, query):
    return spark.read.format('jdbc') \
        .option('url', jdbc_url) \
        .option('user', user) \
        .option('password', password) \
        .option('driver', driver) \
        .option('query', query) \
        .load()

def write_to_local_path(df, file_format, mode, local_path):
    df.write.format(file_format)\
        .mode(mode)\
        .save(local_path)


def write_to_gcs(df, file_format, mode, gcs_path):
    df.write.format(file_format)\
        .option('header', 'true')\
        .mode(mode)\
        .save(f"{gcs_path}")


def write_to_bq(df, bq_table, temp_gcs_bucket, mode):
    df.write.format('bigquery') \
        .option('table', bq_table) \
        .option('temporaryGcsBucket', temp_gcs_bucket) \
        .mode(mode)\
        .save()



