from covid_prophet_dbx.common import Task
from pyspark.sql import DataFrame
import requests


class IngestionTask(Task):
    def _ingest_data(self):
        url = self.conf["input"].get("url")
        db = self.conf["output"].get("database", "default")
        path = self.conf["output"].get("path", "/tmp/covid.csv")
        table = self.conf["output"]["table"]

        self.logger.info(f"Downloading dataset from {url} into {path}")
        response = requests.get(url)
        with open(path, "w") as file:
            file.write(response.text)

        self.logger.info(f"Writing COVID dataset to {db}.{table}")
        df: DataFrame = self.spark.read.csv(
            path.replace("/dbfs", ""),
            header = True,
            inferSchema = True
        )

        # Keep only data from European countries
        df_europe = df.filter("continent = 'Europe'")

        # replace nulls by 0
        df_europe = df_europe.na.fill(0)

        # Write into delta table
        self.spark.sql(f"create database if not exists {db}")
        df_europe.write.saveAsTable(f"{db}.{table}", mode = "overwrite")
        self.logger.info("Dataset successfully written")

    def launch(self):
        self.logger.info("Launching Ingestion task")
        self._ingest_data()
        self.logger.info("Ingestion task finished!")

# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    task = IngestionTask()
    task.launch()

# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == '__main__':
    entrypoint()
