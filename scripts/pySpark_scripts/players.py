#====================================================================================================================================#
#                                                                                                                                    #
#                 *************************************************************************************************                  #
#                 *               This Is pySpark script, used to load data in PostgreSQL database                *                  #
#                 *************************************************************************************************                  #
#                                                                                                                                    #
#             Script Name  = players.py                                                                                              #
#             Description  = This PySpark script reads the 'players.csv' file from HDFS, processes and cleans the data,              #
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
from pyspark.sql.functions import trim,col

#initiate spark session
spark = SparkSession.builder \
    .appName("Players.csv script") \
    .getOrCreate()

#read file from hdfs
players=spark.read.csv('hdfs://localhost:9000/test/players.csv',sep=',',header=True,inferSchema=True)

#change datatype of column
players = players.withColumn("average", col("average").cast("int"))

#trim extra spaces from start and end
players = players.select([ trim(col(c)).alias(c) if players.schema[c].dataType.simpleString() == 'string' else col(c)
    for c in players.columns
])

#load data to postgreSQL database
players.write.format("jdbc")\
 .option("url","jdbc:postgresql://192.168.89.37:5432/myProject")\
 .option("driver","org.postgresql.Driver")\
 .option("dbtable","players")\
 .option("user","hadoop")\
 .option("password","password")\
 .mode('append')\
 .save()

#hadoop command to move file from landing location to archives
subprocess.run(['hdfs','dfs','-mv','/test/players.csv','/Archives'])

print("*****SPARK JOB HAS RUN SUCCESSFULLY.*****")
print("******TRANSFORMATION HAS BEED DONE.******")
print("**PLAYERS.CSV FILE COPIED TO ARCHIVES.**")
print("*********PLAYERS.CSV HAS LOADED.*********")

