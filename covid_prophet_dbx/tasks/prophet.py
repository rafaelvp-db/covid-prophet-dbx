from covid_prophet_dbx.common import Task
from pyspark.sql import DataFrame
from prophet import Prophet
import pandas as pd

class ProphetTask(Task):
    def _fit(
        self,
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

    def _predict(
        self,
        model: Prophet,
        periods = 30
    ):

        # Predict
        future = model.make_future_dataframe(periods=periods)
        result = model.predict(future)
        return result

    def _train_predict(self):
        input_db = self.conf["input"]["database"]
        output_db = self.conf["output"]["database"]
        input_table = self.conf["input"]["table"]
        output_table = self.conf["output"]["table"]
        date_cap = self.conf["train"]["cap"]
        country = self.conf["input"]["country"]

        df: DataFrame = self.spark.sql(f"select * from {input_db}.{input_table}")
        df = df \
                .filter(f'location = "{country}"') \
                .select("date", "new_cases", "new_deaths")

        pandas_df = df.toPandas()
        train_df = pandas_df[pandas_df["date"] <= date_cap]

        m = self._fit(train_df = train_df)
        predictions_df = self._predict(model = m)
        self.logger.info(f"Predictions DF: {predictions_df}")
        spark_predictions_df = self.spark.createDataFrame(predictions_df)
        self.logger.info(f"Returned {len(predictions_df)} predictions")

        # Write into delta table
        self.logger.info("Writing prediction results")
        self.spark.sql(f"create database if not exists {output_db}")
        spark_predictions_df.write.saveAsTable(
            f"{output_db}.{output_table}",
            mode = "overwrite"
        )
        self.logger.info("Predictions successfully written")

    def launch(self):
        self.logger.info("Launching Prophet task")
        self._train_predict()
        self.logger.info("Prophet task finished!")

# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    task = ProphetTask()
    task.launch()

# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == '__main__':
    entrypoint()
