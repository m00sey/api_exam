from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.sql.functions import lit, col, length
import glob
import os
import datetime

class Service():
    def __init__(self):
        self.csv_files = glob.glob(os.path.join(os.getcwd() + '/input-directory', '*.csv'))
        self.col_info = [
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

class Spark_Service():
    def __init__(self):
        self.spark = SparkSession \
                    .builder \
                    .appName("Grism Scoir") \
                    .getOrCreate()
        self.spark.sparkContext.setLogLevel('WARN')

    def file_to_df(self, file, format_of_file, delimeter):
        dfRaw = self.spark.read.load(file, format=format_of_file, sep=delimeter, header= True)
        return dfRaw

    def validate(self, dfRaw, col_info, dfError):
        if not col_info['nullable']:
            dfCorrectData = dfRaw.withColumn(col_info['col_name'], dfRaw[col_info['col_name']].cast(col_info['datatype'])).na.drop(subset=[col_info['col_name']])
            dftemp = dfRaw.exceptAll(dfCorrectData)
            dftemp = dftemp.withColumn("Error Details", lit("{col_name} is null or the data type provided is wrong".format(col_name = col_info['col_name'])))
            dfError = dfError.union(dftemp)

            dfCondition = self.condition(dfRaw, col_info['condition'], col_info['col_name'])
            dfError = dfError.union(dfCondition)
        return dfError

    def condition(self, dfRaw, col_condition, col_name):

        if "len" in col_condition:
            dfTemp = dfRaw.filter(length(col(col_name)) != col_condition['len'])
            dfTemp = dfTemp.withColumn("Error Details", lit("The len of the coloumn does not match the given condition"))

        elif "max_len" in col_condition and "min_len" in col_condition:
            dfTemp = dfRaw.filter(length(col(col_name)) > col_condition['max_len'] & 
                                length(col(col_name)) < col_condition['min_len'])
            dfTemp = dfTemp.withColumn("Error Details", lit("The len of the coloum does not match the given constaints"))

        elif "max_len" in col_condition or "min_len" in col_condition:
            if "max_len" in col_condition:
                dfTemp = dfRaw.filter(length(col(col_name)) > col_condition['max_len'])
                dfTemp = dfTemp.withColumn("Error Details", lit("The len of the coloum does not match the given constaints"))

            elif "min_len" in col_condition:
                dfTemp = dfRaw.filter(length(col(col_name)) < col_condition['min_len'])
                dfTemp = dfTemp.withColumn("Error Details", lit("The len of the coloum does not match the given constaints"))
        
        if "format" in col_condition:
            pass
        
        return dfTemp

if __name__ == "__main__":
    s = Service()

    spark_service = Spark_Service()
    spark = SparkSession \
            .builder \
            .appName("Grism Scoir") \
            .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    for csv in s.csv_files:
        print ("###################################### NEW FILE #########################################")
        print ("csv file:", csv)

        dfRaw = spark_service.file_to_df(csv, 'csv', ',')
        dfRaw = dfRaw.withColumn("Error Details", lit("Everythign is Awesome"))

        dfError = (reduce(lambda dfError, col_info: spark_service.validate(dfRaw, col_info, dfError), s.col_info, dfRaw))
        dfError = dfError.exceptAll(dfRaw)

        if dfError.count() != 0:
            print ("Error File: ", csv)
            dfError.show()
            # put it into error-directory folder

