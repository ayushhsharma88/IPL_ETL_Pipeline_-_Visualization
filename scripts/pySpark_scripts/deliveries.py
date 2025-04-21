#====================================================================================================================================#
#                                                                                                                                    #
#                 *************************************************************************************************                  #
#                 *               This Is pySpark script, used to load data in PostgreSQL database                *                  #
#                 *************************************************************************************************                  ##                                                                                                                                    #
#             Script Name  = deliveries.py                                                                                           #
#             Description  = This PySpark script reads the 'deliveries.csv' file from HDFS, processes and cleans the data,           #
#                            and writes it to a PostgreSQL database.                                                                 #
#                            This script is intended for use in data pipelines where player stats are ingested                       #
#                            from HDFS and stored in a relational database for further analysis or reporting.                        #
#             Arguments    = None                                                                                                    #
#             Dependencies = send_failure_mail, send_success_mail                                                                    #
#             Author       = Ayush Sharma                                                                                            #
#             Email        = myproject.dea@gmail.com                                                                                 #
#             Date         = 18-04-2025 (dd-mm-yyyy format)                                                                          #
#                                                                                                                                    #
#                                                                                                                                    #
#====================================================================================================================================#


from pyspark.sql import SparkSession
import subprocess
from pyspark.sql.functions import trim,col,sum,when,substring
from pyspark.sql.window import Window
from pyspark.sql import functions as F

#initiate spark session
spark = SparkSession.builder \
    .appName("Deliveries.csv script") \
    .getOrCreate()

#extract file from hdfs
deliveries=spark.read.csv('hdfs://localhost:9000/files/deliveries.csv',sep=',',header=True,inferSchema=True)

#trim extra spaces from start and end
deliveries=deliveries.select([ trim(col(c)).alias(c) if deliveries.schema[c].dataType.simpleString() == 'string' else col(c)
    for c in deliveries.columns
])

#create new column using other columns
window_spec=Window.partitionBy("match_id", "inning", "over")
deliveries = deliveries.withColumn("runs_per_over", F.sum("total_runs").over(window_spec))

#load data to postgreSQL database
deliveries.write.format("jdbc")\
 .option("url","jdbc:postgresql://192.168.89.37:5432/myProject")\
 .option("driver","org.postgresql.Driver")\
 .option("dbtable","deliveries")\
 .option("user","hadoop")\
 .option("password","password")\
 .mode('append')\
 .save()

#hadoop command to move file from landing location to archives
subprocess.run(['hdfs','dfs','-mv','/files/deliveries.csv','/archives'])

print("******SPARK JOB HAS RUN SUCCESSFULLY.******")
print("*******TRANSFORMATION HAS BEED DONE.*******")
print("**DELIVERIES.CSV FILE COPIED TO ARCHIVES.**")
print("********DELIVERIES.CSV HAS LOADED.*********")

