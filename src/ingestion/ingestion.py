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
        

    
    