# Databricks notebook source
output_path = "/dbfs/tmp/covid.csv"

!wget https://covid.ourworldindata.org/data/owid-covid-data.csv -O {output_path}

# COMMAND ----------

df = spark.read.csv(output_path.replace("/dbfs", ""), header = True, inferSchema = True)
display(df)

# COMMAND ----------

df_europe = df.filter("continent = 'Europe'")
display(df_europe)

# COMMAND ----------

df_europe = df_europe.na.fill(0)
display(df_europe)

# COMMAND ----------

spark.sql("create database if not exists covid_fc")
df_europe.write.saveAsTable("covid_fc.bronze")
