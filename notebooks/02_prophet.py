# Databricks notebook source
from prophet import Prophet
import pandas as pd

# COMMAND ----------

df = spark.sql("select * from covid_fc.bronze")
display(df)

# COMMAND ----------

df = df.filter('location = "United Kingdom"').select("date", "new_cases", "new_deaths")
pandas_df = df.toPandas()

train_df = pandas_df[pandas_df["date"] <= '2022-07-01']
test_df = pandas_df[pandas_df["date"] > '2022-07-01']

print(f"Training set has {len(train_df)} rows")
print(f"Testing set has {len(test_df)} rows")

# COMMAND ----------

def fit(
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

  # Trauin
  m.fit(train_df)
  return m

def predict(model: Prophet, periods = 30):

  # Predict
  future = model.make_future_dataframe(periods=periods)
  result = model.predict(future)
  return result

# COMMAND ----------

m = fit(train_df)
forecast = predict(m)
m.plot_components(forecast)

# COMMAND ----------

import seaborn as sns
from matplotlib import pyplot as plt

sns.scatterplot(x = forecast.ds, y = forecast.yhat, label = "predicted")
sns.scatterplot(x = train_df.date, y = train_df.new_cases, label = "actual")
plt.xticks(rotation=45)
plt.title("In Sample Forecast")

# COMMAND ----------

test_df = test_df.rename(columns = {"date": "ds"})
pred = m.predict(test_df)

sns.scatterplot(x = pred.ds, y = pred.yhat, label = "predicted")
sns.scatterplot(x = test_df.ds, y = test_df.new_cases, label = "actual")
plt.xticks(rotation=45)
plt.title("Out of Sample Forecast")

# COMMAND ----------

spark.sql("create database if not exists covid_fc")
output_df = spark.createDataFrame(pred)
output_df.write.saveAsTable("covid_fc.prediction")

# COMMAND ----------


