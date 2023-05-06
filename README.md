**DEPENDENCIES**

Following python libraries are required:

-pyspark

-google-cloud-storage

-google-cloud-bigquery

-pytest

Following JAR dependencies should be downloaded and provided in config file in the specified field:

-Google Cloud BigQuery Spark Connector (https://github.com/GoogleCloudDataproc/spark-bigquery-connector/releases)
-Google Cloud Storage Hadoop Connector (https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage#non-dataproc_clusters)
-JDBC Driver of the desired jdbc compliant database

**HOW TO RUN**

A GCP Service account key file should be downloaded. The service account can be worked with following permissions:

- BigQuery DataEditor
- BigQuery Job User
- Cloud Storage Admin

Open a dataset on BQ and at least 1 bucket on GCP 

Create a project and a virtual environment with specified dependencies.

Fill the configuration config.json file as specified in the file.

The main function resides in JDBCExporter.py

Function can be called in the following pattern:

python JDBCExporter.py --config config.json --gcs True --bq True


**HOW TO TEST**

test_utils.py contains the unit test functions. Pytest can be used for testing.