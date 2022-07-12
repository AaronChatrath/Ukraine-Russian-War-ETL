from pyspark.sql import SparkSession

#Create a dataframe from the csv filled
def load_s3_data(path, sp):
    
    args = {
        'path': path,
        'header': 'True',
        'inferSchema': 'True'
    }

    return sp.read.csv(**args)