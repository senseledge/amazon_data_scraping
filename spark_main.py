from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("DataScraping").getOrCreate()

textFile = spark.read.text("prova.md")
count = textFile.count()
print(count)

spark.stop()
