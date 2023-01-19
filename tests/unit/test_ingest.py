from covid_prophet_dbx.tasks.ingest import IngestionTask
from pyspark.sql import SparkSession
import logging

def test_ingest(spark: SparkSession):
    logging.info("Testing the ETL job")
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