from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.sql.functions import lit, col, length

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
    
    def write(self, dataframe, write_file, format_of_file, repartition, delimiter):
        dataframe.repartition(repartition).write.format(format_of_file).option("header", True).option("delimiter", delimiter).save(write_file)

    def validate(self, dfRaw, col_info, dfError):
        if not col_info['nullable']:
            dfCorrectData = dfRaw.withColumn(col_info['col_name'], dfRaw[col_info['col_name']].cast(col_info['datatype'])).na.drop(subset=[col_info['col_name']])
            dftemp = dfRaw.exceptAll(dfCorrectData)
            dftemp = dftemp.withColumn("ERROR_MSG", lit("{col_name} is null or the data type provided is wrong".format(col_name = col_info['col_name'])))
            dfError = dfError.union(dftemp)

        dfCondition = self.condition(dfRaw, col_info['condition'], col_info['col_name'])
        dfError = dfError.union(dfCondition)
        return dfError

    def condition(self, dfRaw, col_condition, col_name):

        if "len" in col_condition:
            dfTemp = dfRaw.filter(length(col(col_name)) != col_condition['len'])
            dfTemp = dfTemp.withColumn("ERROR_MSG", lit("The len of the coloumn does not match the given condition"))

        elif "max_len" in col_condition and "min_len" in col_condition:
            dfTemp = dfRaw.filter(length(col(col_name)) > col_condition['max_len'] & 
                                length(col(col_name)) < col_condition['min_len'])
            dfTemp = dfTemp.withColumn("ERROR_MSG", lit("The len of the coloum does not match the given condition"))

        elif "max_len" in col_condition or "min_len" in col_condition:
            if "max_len" in col_condition:
                dfTemp = dfRaw.filter(length(col(col_name)) > col_condition['max_len'])
                dfTemp = dfTemp.withColumn("ERROR_MSG", lit("The len of the coloum does not match the given condition"))

            elif "min_len" in col_condition:
                dfTemp = dfRaw.filter(length(col(col_name)) < col_condition['min_len'])
                dfTemp = dfTemp.withColumn("ERROR_MSG", lit("The len of the coloum does not match the given condition"))
        
        if "format" in col_condition:
            dfCorrect = dfRaw.filter(col(col_name).rlike(col_condition['format']))
            dfTemp = dfRaw.exceptAll(dfCorrect)
            dfTemp = dfTemp.withColumn("ERROR_MSG", lit("Given format does not match."))
            pass
        
        return dfTemp
