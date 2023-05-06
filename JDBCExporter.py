import argparse
from utils import *


if __name__ == "__main__":

    # Define command-line arguments
    parser = argparse.ArgumentParser(
        description='Export data from JDBC compliant database to local path, Google Cloud Storage, and/or BigQuery.')
    parser.add_argument('--config', required=True, help='Path to configuration file.')
    parser.add_argument('--gcs', required=True, help='Export data to Google Cloud Storage.')
    parser.add_argument('--bq', required=True, help='Export data to BigQuery.')
    args = parser.parse_args()

    # Load configuration file
    try:
        config = read_config(args.config)
    except Exception as e:
        raise Exception("Failed to load config file: {}".format(e))

    # Create Spark session
    try:
        jdbc_driver_path = config['source']['jdbc_driver_path']
        gcs_con_path = config['output']['gcs']['connector_path']
        bq_con_path = config['output']['bq']['connector_path']
        gcp_credentials_file = config['gcp']['service_account_key_file_path']

        spark = get_spark_session(jdbc_driver_path, gcs_con_path, bq_con_path, gcp_credentials_file)
    except Exception as e:
        raise Exception("Failed when creating Spark Session: {}".format(e))

    try:
        # Set source configurations
        jdbc_url = config['source']['url']
        user = config['source']['user']
        password = config['source']['password']
        driver = config['source']['driver']
        query = config['source']['query']

        # Read data from database into a PySpark DataFrame
        df = read_data(spark, jdbc_url, user, password, driver, query)
    except Exception as e:
        raise Exception("Failed to read data: {}".format(e))

    # Write data to local path
    try:
        local_path = config['output']['local']['path']
        local_file_format = config['output']['local']['format']
        local_mode = config['output']['local']['mode']
        write_to_local_path(df, local_file_format, local_mode, local_path)
    except Exception as e:
        raise Exception("Failed when writing data to local path: {}".format(e))

    # Write data to Google Cloud Storage
    if args.gcs.lower() == "true":
        try:
            gcs_path = config['output']['gcs']['path']
            gcs_file_format = config['output']['gcs']['format']
            gcs_mode = config['output']['gcs']['mode']
            write_to_gcs(df, gcs_file_format, gcs_mode, gcs_path)
        except Exception as e:
            raise Exception("Failed when writing data to GCS: {}".format(e))

    # Write data to BigQuery
    if args.bq.lower() == "true":
        try:
            bq_table = config['output']['bq']['table']
            bq_mode = config['output']['bq']['mode']
            temp_gcs_bucket = config['output']['bq']['temporary_gcs_bucket']
            write_to_bq(df, bq_table, temp_gcs_bucket, bq_mode)
        except Exception as e:
            raise Exception( "Failed when writing data to GCS: {}".format(e))

    # Stop Spark session
    spark.stop()
