from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.sql.functions import lit
import glob
import os
import datetime

csv_files = glob.glob(os.path.join(os.getcwd() + '/input-directory', '*.csv'))

spark = SparkSession \
        .builder \
        .appName("Grism Scoir") \
        .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

col_info = [
        {
            "col_name": "INTERNAL_ID",
            "datatype": "int",
            "nullable": False,
            "condition": {
                "len": 8
            }
        },
        {
            "col_name": "FIRST_NAME",
            "datatype": "string",
            "nullable": False,
            "condition": {
                "max_len": 15
            }
        },
        {
            "col_name": "MIDDLE_NAME",
            "datatype": "string",
            "nullable": True,
            "condition": {
                "max_len": 15
            }
        },
        {
            "col_name": "LAST_NAME",
            "datatype": "string",
            "nullable": False,
            "condition": {
                "max_len": 15
            }
        },
        {
            "col_name": "PHONE_NUM",
            "datatype": "string",
            "nullable": False,
            "condition": {
                "max_len": 15
            }
        }
    ]

def nulls_and_datatypes(dfRaw, col_info, dfError):
    if not col_info['nullable']:
        dfCorrectData = dfRaw.withColumn(col_info['col_name'], dfRaw[col_info['col_name']].cast(col_info['datatype'])).na.drop(subset=[col_info['col_name']])
        dftemp = dfRaw.exceptAll(dfCorrectData)
        dftemp = dftemp.withColumn("Error Details", lit("{col_name} is null or the data type provided is wrong".format(col_name = col_info['col_name'])))
        dfError = dfError.union(dftemp)
    return dfError

for csv in csv_files:
    print ("###################################### NEW FILE #########################################")
    print ("csv file:", csv)

    dfRaw = spark.read.load(csv, format='csv', sep=',', header= True)
    dfRaw = dfRaw.withColumn("Error Details", lit("Everythign is Awesome"))

    dfError = (reduce(lambda dfError, col_info: nulls_and_datatypes(dfRaw, col_info, dfError), col_info, dfRaw))
    dfError = dfError.exceptAll(dfRaw)

    if dfError.count() != 0:
        print ("Error File: ", csv)
        dfError.show()
        # put it into error-directory folder