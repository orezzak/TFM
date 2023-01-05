# Databricks notebook source
# MAGIC %sql
# MAGIC select * from bronze.tweets 
# MAGIC order by 2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.tweets
# MAGIC order by 2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.tweets
# MAGIC --where author_id='142296675'