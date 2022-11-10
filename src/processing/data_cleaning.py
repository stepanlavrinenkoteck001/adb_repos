from pyspark.sql import DataFrame
from math import pi

class cleanData:
    def __init__(self, df: DataFrame, columns = list[str, str]):
        self.df = df
        self.geom_columns = columns
        
    def drop_nulls(self):    
        self.df = self.df[self.geom_columns].na.drop()
        return df