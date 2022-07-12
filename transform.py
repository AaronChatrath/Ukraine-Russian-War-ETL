from ast import Eq
from json import load
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext, functions as F
from pyspark.sql.window import Window


def data_pull(dataframe, selectStatement):
    return dataframe.select(selectStatement)

#all the values in the dataframe are cumulative, this function makes it so it doesn't keep adding each new row value again and again.
def per_day_df_creation(orderby, dataframe, column_name, new_per_day_column_name):
    my_window = Window.partitionBy().orderBy(orderby)
    #create a column that has the previous row value 
    dataframe = dataframe.withColumn("per_day_column_prev_value", F.lag(column_name).over(my_window))
    #minus the current row value from the prior row value
    dataframe= dataframe.withColumn(new_per_day_column_name, F.when(F.isnull(column_name - dataframe.per_day_column_prev_value), column_name)
                        .otherwise(column_name - dataframe.per_day_column_prev_value))
    dataframe = dataframe.drop(column_name)
    dataframe = dataframe.drop(F.col("per_day_column_prev_value"))
    return dataframe