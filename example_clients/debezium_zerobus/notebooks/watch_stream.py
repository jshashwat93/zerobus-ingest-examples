# Databricks notebook source
# MAGIC %md
# MAGIC # Watch the Debezium Zerobus stream arrive
# MAGIC
# MAGIC A `SELECT` in the SQL editor shows a snapshot, so seeing new rows means running
# MAGIC it again. This reads the target table as a stream instead, so rows appear as
# MAGIC Debezium sends them without you doing anything.
# MAGIC
# MAGIC Set the catalog and schema below to match your `.env`, attach serverless or any
# MAGIC cluster, and run all. Leave `docker compose up` running while you watch.
# MAGIC
# MAGIC Zerobus only ever appends, including for updates and deletes, so a plain
# MAGIC streaming read sees every change without Change Data Feed being enabled.

# COMMAND ----------

catalog = "main"
schema = "zerobus_cdc"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders, as they arrive
# MAGIC
# MAGIC An insert and a later update to the same row both appear, as two rows with the
# MAGIC same `order_id` and different `updated_at`. A delete appears as a row with
# MAGIC `__deleted` set to the string `"true"`.

# COMMAND ----------

orders = spark.readStream.table(f"{catalog}.{schema}.orders")
display(orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers, as they arrive

# COMMAND ----------

customers = spark.readStream.table(f"{catalog}.{schema}.customers")
display(customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stopping
# MAGIC
# MAGIC Each `display()` above holds an active stream open. Use *Stop* on the cell, or
# MAGIC run the cell below to stop all of them, otherwise they keep the compute busy.

# COMMAND ----------

for query in spark.streams.active:
    query.stop()
