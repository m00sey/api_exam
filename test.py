from spark_service import Spark_Service
from service import Service

from pyspark.sql.functions import lit, col, length
import os

if __name__ == "__main__":
    s = Service()
    spark_service = Spark_Service()

    for csv in s.csv_files:
        
        csv_fileName = csv.split('/')[-1]
        if not csv_fileName in s.processed_dicts:
            print ("###################################### NEW FILE #########################################")
            print ("INFO: File to Validate: ", csv)

            dfRaw = spark_service.file_to_df(csv, 'csv', ',')
            print ("INFO: Number of Rows in the file: ", dfRaw.count())
            dfRaw = dfRaw.withColumn("ERROR_MSG", lit("Everything is Awesome"))

            dfError = (reduce(lambda dfError, col_info: spark_service.validate(dfRaw, col_info, dfError), s.col_info, dfRaw))
            dfError = dfError.exceptAll(dfRaw)

            if dfError.count() != 0:
                print ("INFO: The file that failed Validation: ", csv)
                print ("INFO: Number of rows Failed in Validation: ", dfError.count())
                dfError.show()
                spark_service.write(dfError, os.getcwd() + '/error-directory/' + csv.split('/')[-1], 'csv', 1, ',')
            else:
                dfRaw = dfRaw.drop('ERROR_MSG')
                spark_service.write(dfRaw, os.getcwd() + '/output-directory/' + csv.split('/')[-1], 'json', 1, ',')
            s.move_file(csv, os.getcwd() + '/processed/' + csv.split('/')[-1])
        else:
            print ("INFO: All ready processed", csv)
            os.remove(csv)
