# Databricks notebook source
# MAGIC %md
# MAGIC ## Pull repo from VS code and import code back to ADB

# COMMAND ----------

path = '/mnt/dalz/cda-blastiq/tbl_blastiq_hole'
geom_columns=["design_diameter_hole","design_length_hole"]


# COMMAND ----------

from src.ingestion.ingestion import blastIqIngest

# COMMAND ----------

ingestor = blastIqIngest(path)
df = ingestor.ingest()


# COMMAND ----------

from src.processing.data_cleaning import cleanData

# COMMAND ----------

cleaner = cleanData(df, geom_columns)
df = cleaner.drop_nulls()


# COMMAND ----------

from src.processing.geom_utils import holeGeometry

# COMMAND ----------

geometry = holeGeometry(df, geom_columns)
df = geometry.calculate_hole_volume().display()

# COMMAND ----------



# COMMAND ----------


