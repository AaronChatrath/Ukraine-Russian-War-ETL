from json import load
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext, functions as F
from pyspark.sql.window import Window
import os
import socket

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.2 pyspark-shell'

spark = SparkSession.builder.master("local[*]").getOrCreate()

hadoopConf=spark.sparkContext._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.access.key", "YOUR-ACCESS-KEY")
hadoopConf.set("fs.s3a.secret.key", "YOUR-SECRET-KEY")

spark.range(100, numPartitions=100).rdd.map(lambda x: socket.gethostname()).distinct().collect()

def load_s3_data(path):
    """Create a dataframe from the csv filled"""

    args = {
        'path': path,
        'header': 'True',
        'inferSchema': 'True'
    }

    return spark.read.csv(**args)

def data_pull(dataframe, selectStatement):
    return dataframe.select(selectStatement)

select_list_personnel = ["date", "personnel"]
select_list_equipment = ["date", "aircraft", "drone","helicopter", "tank", "APC"]

cum_rus_eq_losses = load_s3_data("s3a://NameOfYourS3Bucket/russia_losses_equipment.csv")
cum_rus_pers_losses = load_s3_data("s3a://NameOfYourS3Bucket/russia_losses_personnel.csv")


personnel_losses = data_pull(cum_rus_pers_losses, select_list_personnel)
equipment_losses = data_pull(cum_rus_eq_losses, select_list_equipment)


print(personnel_losses.show(3))
print(equipment_losses.show(3))