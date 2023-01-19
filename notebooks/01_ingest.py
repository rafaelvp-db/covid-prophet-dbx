# Databricks notebook source
import requests

url = 'https://covid.ourworldindata.org/data/owid-covid-data.csv'
output_path = '/dbfs/tmp/covid.csv'

response = requests.get(url)
with open(output_path, "w") as file:
  file.write(response.text)
  
df = spark.read.csv(output_path.replace("/dbfs", ""), header = True, inferSchema = True)
display(df)

# Changes

# COMMAND ----------

df_europe = df.filter("continent = 'Europe'")
display(df_europe)

# COMMAND ----------

df_europe = df_europe.na.fill(0)
display(df_europe)

# COMMAND ----------

spark.sql("create database if not exists covid_fc")
df_europe.write.saveAsTable("covid_fc.bronze", mode = "overwrite")
