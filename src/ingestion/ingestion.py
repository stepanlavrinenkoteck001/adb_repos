from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

class blastIqIngest:
    def __init__(self, path = str):
        self.path = path
        
    def ingest(self):
        # read in deafault databricks dataset
        df = (spark.read.format('delta')
            .options(header='true', inferSchema='true')
            .load(self.path))
        return df

            
        

    
    