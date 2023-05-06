import shutil
from utils import *
from pyspark.sql import SparkSession
from google.cloud import storage
from google.cloud import bigquery

import os


config_file = "test_data/test_config.json"


def test_read_config():
    config = read_config(config_file)
    assert list(config.get("gcp").keys())[0] == "service_account_key_file_path"
    assert len(list(config.get("gcp").values())) == 1


def test_get_spark_session():
    config = read_config(config_file)
    jdbc_driver_path = config['source']['jdbc_driver_path']
    gcs_con_path = config['output']['gcs']['connector_path']
    bq_con_path = config['output']['bq']['connector_path']
    gcp_credentials_file = config['gcp']['service_account_key_file_path']

    try:
        spark = get_spark_session(jdbc_driver_path, gcs_con_path, bq_con_path, gcp_credentials_file)
    except:
        assert False
    else:
        assert type(spark) == SparkSession
        spark.stop()


config = read_config(config_file)
jdbc_driver_path = config['source']['jdbc_driver_path']
gcs_con_path = config['output']['gcs']['connector_path']
bq_con_path = config['output']['bq']['connector_path']
gcp_credentials_file = config['gcp']['service_account_key_file_path']
spark = get_spark_session(jdbc_driver_path, gcs_con_path, bq_con_path, gcp_credentials_file)
test_df = spark.read.csv("test_data/test_data.csv")


def test_read_data():

    jdbc_url = config['source']['url']
    user = config['source']['user']
    password = config['source']['password']
    driver = config['source']['driver']
    query = config['source']['query']

    spark = get_spark_session(jdbc_driver_path, gcs_con_path, bq_con_path, gcp_credentials_file)
    df = read_data(spark, jdbc_url, user, password, driver, query)

    assert df.count() == 200
    assert len(df.columns) == 4


def test_write_to_local():
    shutil.rmtree("exports")
    write_to_local_path(test_df, 'csv', "overwrite", "exports")

    assert os.path.exists("exports")
    files = os.listdir("exports")

    assert len(files) > 0


def test_write_to_gcs():
    # Set credentials
    client = storage.Client.from_service_account_json(gcp_credentials_file)

    # Set bucket name and folder path
    gcs_path = config["output"]["gcs"]["path"]
    bucket_name = gcs_path.replace("gs://","").split("/")[0]
    folder_path = gcs_path.replace("gs://","").split("/")[1]
    # Get bucket and list all files under the folder
    bucket = client.bucket(bucket_name)
    if folder_path in bucket.list_blobs(prefix=folder_path):
        folder = bucket.blob(folder_path)
        folder.delete()

    write_to_gcs(test_df, 'csv', "overwrite", gcs_path)
    blobs = bucket.list_blobs(prefix=folder_path)
    num_files = sum(1 for _ in blobs)

    assert num_files > 0


def test_write_to_bq():
    client = bigquery.Client.from_service_account_json(gcp_credentials_file)

    bq_table = config['output']['bq']['table']
    temp_gcs_bucket = config['output']['bq']['temporary_gcs_bucket']
    write_to_bq(test_df, bq_table, temp_gcs_bucket, "overwrite")

    query = "SELECT count(1) as row_count FROM {}".format(bq_table)

    query_job = client.query(query)
    results = query_job.result()

    for row in results:
        assert row.row_count == 109
        break

