import sys
from pyspark.sql import SparkSession 

sys.path.append("data")

spark = SparkSession.builder.master("local[*]").getOrCreate()

file_csv = 'data/UCI_Credit_Card.csv'

data_file = spark.read.csv(file_csv, header=True)

# Enseñar 20 primeras observaciones
data_file.show()


data_file