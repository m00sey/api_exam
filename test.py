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

    dfRaw = spark.read.load(csv, format='csv', sep=',', header= True)

    print ("File Count: ", dfRaw.count())

    # checking for nulls and datatype
    dfCorrectData = dfRaw.withColumn("INTERNAL_ID", dfRaw["INTERNAL_ID"].cast("int")).na.drop(subset=["INTERNAL_ID"])
    dfError = dfRaw.exceptAll(dfCorrectData)

    if dfError.count() > 1:
        print ("Error File: ", csv)
        # put it into error-directory folder