# Databricks notebook source
from prophet import Prophet
import pandas as pd

# COMMAND ----------

df = spark.sql("select * from covid_fc.bronze")
display(df)

# COMMAND ----------

df = df.filter('location = "United Kingdom"').select("date", "new_cases", "new_deaths")
pandas_df = df.toPandas()

train_df = pandas_df[pandas_df["date"] <= '2022-08-01']
test_df = pandas_df[pandas_df["date"] > '2022-08-01']

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
forecast = predict(m, periods = 30)
m.plot_components(forecast)

# COMMAND ----------

import seaborn as sns
from matplotlib import pyplot as plt

sns.scatterplot(
  x = forecast[forecast['ds'] > '2022-08-01'].ds,
  y = forecast[forecast['ds'] > '2022-08-01'].yhat,
  label = "predicted"
)

sns.scatterplot(
  x = test_df[test_df['date'] < '2022-09-01'].date,
  y = test_df[test_df['date'] < '2022-09-01'].new_cases,
  label = "actual"
)
plt.xticks(rotation=45)
plt.title("Actual x Forecasted")
