from covid_prophet_dbx.common import Task
from pyspark.sql import DataFrame
from prophet import Prophet
import pandas as pd


class ProphetTask(Task):
    def _fit(
        train_df: pd.DataFrame,
        weekly_seasonality: bool = True,
        daily_seasonality: bool = True
        ):

        m = Prophet(
            weekly_seasonality = weekly_seasonality,
            daily_seasonality = daily_seasonality
        )
        train_df = (
            train_df
            .loc[:, ["date", "new_cases"]]
            .rename(columns = {"date": "ds", "new_cases": "y"})
        )

        train_df["ds"] = pd.to_datetime(train_df["ds"])

        # Train
        m.fit(train_df)
        return m

    def predict(model: Prophet, periods = 30):

        # Predict
        future = model.make_future_dataframe(periods=periods)
        result = model.predict(future)
        return result
        
    def _train_predict(self):
        db = self.conf["input"].get("database", "default")
        table = self.conf["input"]["table"]
        date_cap = self.conf["train"]["cap"]
        country = self.conf["input"]["country"]

        df: DataFrame = self.spark.sql(f"select * from {db}.{table}")
        df = df.filter(f'location = "{country}"').select("date", "new_cases", "new_deaths")
        pandas_df = df.toPandas()
        train_df = pandas_df[pandas_df["date"] <= date_cap]
        test_df = pandas_df[pandas_df["date"] > date_cap]

        # Write into delta table
        self.spark.sql(f"create database if not exists {db}")
        df_predictions.write.saveAsTable(f"{db}.{table}", mode = "overwrite")
        self.logger.info("Predictions successfully written")

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
