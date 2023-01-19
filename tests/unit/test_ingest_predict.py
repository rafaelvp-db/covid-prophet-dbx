from covid_prophet_dbx.tasks.ingest import IngestionTask
from covid_prophet_dbx.tasks.prophet import ProphetTask
from pyspark.sql import SparkSession
import logging

def test_ingest_predict(spark: SparkSession):

    # Ingestion
    logging.info("Testing the Ingestion job")
    output_config = {"database": "covid_fc", "table": "bronze"}
    input_config = {"url": 'https://covid.ourworldindata.org/data/owid-covid-data.csv'}
    test_ingest_config = {
        "output": output_config,
        "input": input_config
    }
    ingest_job = IngestionTask(spark, test_ingest_config)
    ingest_job.launch()
    table_name = f"{test_ingest_config['output']['database']}.{test_ingest_config['output']['table']}"
    _count = spark.table(table_name).count()

    assert _count > 0

    # Prediction
    logging.info("Testing the Prophet job")
    output_config = {
        "database": "covid_fc",
        "table": "prediction"
    }
    train_config = {
        "cap": "2022-08-01"
    }
    input_config = {
        "database": "covid_fc",
        "table": "bronze",
        "country": "United Kingdom"
    }
    test_prophet_config = {
        "output": output_config,
        "train": train_config,
        "input": input_config
    }
    prophet_job = ProphetTask(spark, test_prophet_config)
    prophet_job.launch()
    table_name = f"{output_config['database']}.{output_config['table']}"
    _count = spark.sql(f"select * from {table_name}").count()

    assert _count > 0