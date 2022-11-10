from pyspark.sql import DataFrame
from math import pi


class holeGeometry:
    def __init__(self, df: DataFrame, columns = list[str, str]):
        self.df = df
        self.geom_columns = columns
        
    @staticmethod
    def volume_formula(diameter, height):
        volume = pi * (diameter / 2) **2 * height
        return volume
        
    def calculate_hole_volume(self):
        if len(self.geom_columns) == 2:
            i_diameter = [i for i in [0,1] if self.geom_columns[i].find('diameter')>0][0]
            df_volume = self.df.select(volume_formula(self.df[self.geom_columns[i_diameter]], self.df[self.geom_columns[~i_diameter]]))
            df_volume = df_volume.withColumnRenamed(df_volume.schema.names[0], 'volume')
            return df_volume
        else:
            print('length of geometry columns passed > 2')
            
    
