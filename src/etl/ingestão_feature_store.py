# Databricks notebook source
def import_query(path: str):
    with open(path, 'r') as open_file:
        return open_file.read()

table = "vendas"
database = "silver.analytics"
table_name = f"{database}.fs_vendedor_{table}"
query = import_query(f'{table}.sql')

df = spark.sql(query.format(date='2018-01-01'))
df.display()

# COMMAND ----------


