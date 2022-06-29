from ast import Eq
from json import load
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext, functions as F
from pyspark.sql.window import Window
import os
import socket
from transform import *
from loads3 import *
from databaseStructure import *
from cgi import test
from unicodedata import name
import mysql.connector
from mysql.connector import errorcode
import sys
import pandas as pd

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.2 pyspark-shell'

spark = SparkSession.builder.master("local[*]").getOrCreate()

hadoopConf=spark.sparkContext._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.access.key", "YOUR-ACCESS-KEY")
hadoopConf.set("fs.s3a.secret.key", "YOUR-SECRET-KEY")

spark.range(100, numPartitions=100).rdd.map(lambda x: socket.gethostname()).distinct().collect()


select_list_personnel = ["date", "personnel"]
select_list_equipment = ["date", "aircraft", "drone","helicopter", "tank", "APC"]

cum_rus_eq_losses = load_s3_data("s3a://YOUR-BUCKET-NAME/russia_losses_equipment.csv", spark)
cum_rus_pers_losses = load_s3_data("s3a://YOUR-BUCKET-NAME/russia_losses_personnel.csv", spark)


personnel_losses = data_pull(cum_rus_pers_losses, select_list_personnel)
equipment_losses = data_pull(cum_rus_eq_losses, select_list_equipment)


print(personnel_losses.show(3))
print(equipment_losses.show(3))

equipment_losses = per_day_df_creation("date", equipment_losses, equipment_losses.aircraft, "aircraft_per_day")
equipment_losses = per_day_df_creation("date", equipment_losses, equipment_losses.drone, "drone_per_day")
equipment_losses = per_day_df_creation("date", equipment_losses, equipment_losses.helicopter, "helicopter_per_days")
equipment_losses = per_day_df_creation("date", equipment_losses, equipment_losses.tank, "tank_per_days")
equipment_losses = per_day_df_creation("date", equipment_losses, equipment_losses.APC, "apc_per_days")

personnel_losses = per_day_df_creation("date", personnel_losses, personnel_losses.personnel, "personnel_per_day")

print(equipment_losses.show(10))
print(personnel_losses.show(10))

print(equipment_losses.schema)
print(personnel_losses.schema)

cum_rus_pers_losses.createOrReplaceTempView("cum_rus_pers")
cum_rus_eq_losses.createOrReplaceTempView("cum_rus_eq")
cumulative_losses = spark.sql("SELECT cum_rus_pers.date, cum_rus_pers.personnel,cum_rus_eq.aircraft,cum_rus_eq.drone,cum_rus_eq.helicopter,cum_rus_eq.tank,cum_rus_eq.APC FROM cum_rus_pers Full OUTER JOIN cum_rus_eq ON cum_rus_pers.date == cum_rus_eq.date")
print(cumulative_losses.show(10))

create_database("RUSUKRWAR")
create_tables()
insert_data(personnel_losses, equipment_losses, cumulative_losses)