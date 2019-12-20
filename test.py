from pyspark.sql import SparkSession, Row, SQLContext
import glob
import os
import datetime

csv_files = glob.glob(os.path.join(os.getcwd() + '/input-directory', '*.csv'))

spark = SparkSession \
        .builder \
        .appName("Grism Scoir") \
        .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

for csv in csv_files:
    print ("csv file:", csv)
