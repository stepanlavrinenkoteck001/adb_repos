# Databricks notebook source
# MAGIC %md
# MAGIC ## Refactoring code on ADB + copy-pasting to VS code

# COMMAND ----------

path = '/mnt/dalz/cda-blastiq/tbl_blastiq_hole'

# COMMAND ----------

# read in deafault databricks dataset
df = (spark.read.format('delta')
            .options(header='true', inferSchema='true')
            .load(path))


# COMMAND ----------

df.display()

geom_columns=["design_diameter_hole","design_length_hole"]


# COMMAND ----------

df['design_diameter_hole', 'design_length_hole'].display()

# COMMAND ----------

from pyspark.sql.functions import col,isnan,when,count

df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in geom_columns]
   ).show()

# COMMAND ----------

df = df[geom_columns].na.drop()
# df = df[geom_columns].na.drop()
df.display()

# COMMAND ----------



# COMMAND ----------

import numpy as np
# Volume of a cylinder
# V = pi * r^2 * h = pi * (D/2)*2 * h
volume = df.select(np.pi * (df[geom_columns[0]] / 2) **2 *df[geom_columns[1]])


# COMMAND ----------

df.display()

# COMMAND ----------

volume.schema.names[0]

# COMMAND ----------

volume = volume.withColumnRenamed(volume.schema.names[0], 'volume')

# COMMAND ----------

volume.display()

# COMMAND ----------

df[geom_columns].limit(1).display()
# check (0.2/2)^2*3.14*11.66 = 0.366

# COMMAND ----------

[i for i in [0,1] if geom_columns[i].find('diameter')>0]

# COMMAND ----------

geom_columns[~0]

# COMMAND ----------

from math import pi
i_diameter = [i for i in [0,1] if geom_columns[i].find('diameter')>0][0]
diameter = df[geom_columns[i_diameter]]
height = df[geom_columns[~i_diameter]]

def volume_formula(diameter, height):
    volume = pi * (diameter / 2) **2 * height
    return volume

df.select(volume_formula(diameter, height)).display()


# COMMAND ----------

class blastIqIngest:
    def __init__(self, path = str):
        self.path = path
        
    def ingest(self):
        try:
            # read in deafault databricks dataset
            df = (spark.read.format('delta')
                .options(header='true', inferSchema='true')
                .load(self.path))
            return df
        except:
            print('blast Iq data not loaded properly')
        

    
    

# COMMAND ----------

ingestor = blastIqIngest(path)
ingestor.ingest().limit(5).display()

# COMMAND ----------

from pyspark.sql import DataFrame

class cleanData:
    def __init__(self, df: DataFrame, geom_columns = list[str, str]):
        self.df = df
        self.geom_columns = geom_columns
        
    def drop_nulls(self):    
        self.df = self.df[self.geom_columns].na.drop()
        return df

# COMMAND ----------

cleaner = cleanData(df, geom_columns)
df = cleaner.drop_nulls().limit(5).display()


# COMMAND ----------

from pyspark.sql import DataFrame
from math import pi


class holeGeometry:
    def __init__(self, df: DataFrame, geom_columns = list[str, str]):
        self.df = df
        self.geom_columns = geom_columns
        
    @staticmethod
    def volume_formula(diameter, height):
        volume = pi * (diameter / 2) **2 * height
        return volume
        
    def calculate_hole_volume(self):
        if len(geom_columns) == 2:
            i_diameter = [i for i in [0,1] if self.geom_columns[i].find('diameter')>0][0]
            df_volume = df.select(volume_formula(self.df[self.geom_columns[i_diameter]], self.df[self.geom_columns[~i_diameter]]))
            df_volume = df_volume.withColumnRenamed(df_volume.schema.names[0], 'volume')
            return df_volume
        else:
            print('length of geometry columns passed > 2')
            
    


# COMMAND ----------



# COMMAND ----------

geometry = holeGeometry(df, geom_columns)

# COMMAND ----------

df = geometry.calculate_hole_volume()

# COMMAND ----------

df.display()

# COMMAND ----------

geometry.volume_formula()

# COMMAND ----------


