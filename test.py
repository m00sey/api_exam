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

def nulls_and_datatypes(dfRaw, col_name, dfError):
    dfCorrectData = dfRaw.withColumn(col_name, dfRaw[col_name].cast("int")).na.drop(subset=[col_name])
    dftemp = dfRaw.exceptAll(dfCorrectData)
    dfError = dfError.union(dftemp)
    return dfError

for csv in csv_files:
    print ("csv file:", csv)

    dfRaw = spark.read.load(csv, format='csv', sep=',', header= True)

    print ("File Count: ", dfRaw.count())

    col_name = ["INTERNAL_ID", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "PHONE_NUM"]
    dfError = (reduce(lambda dfError, col_name: nulls_and_datatypes(dfRaw, col_name, dfError), col_name, dfRaw))
    dfError = dfError.exceptAll(dfRaw)

    if dfError.count() > 1:
        print ("Error File: ", csv)
        dfError.show()
        # put it into error-directory folder