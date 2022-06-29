from pyspark.sql import SparkSession

def load_s3_data(path, sp):
    """Create a dataframe from the csv filled"""

    args = {
        'path': path,
        'header': 'True',
        'inferSchema': 'True'
    }

    return sp.read.csv(**args)