#====================================================================================================================================#
#                                                                                                                                    #
#                 *************************************************************************************************                  #
#                 *               This Is pySpark script, used to load data in PostgreSQL database                *                  #
#                 *************************************************************************************************                  ##                                                                                                                                    #
#             Script Name  = matches.py                                                                                              #
#             Description  = This PySpark script reads the 'matches.csv' file from HDFS, processes and cleans the data,              #
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
from pyspark.sql.functions import when,trim,col,substring

#initiate spark session
spark = SparkSession.builder \
    .appName("Matches.csv script") \
    .getOrCreate()

#read file from hdfs
matches=spark.read.csv('hdfs://localhost:9000/files/matches.csv',sep=',',header=True,inferSchema=True)

#extract year from date column and create a new column
matches=matches.withColumn('season',substring("date",7,4))

#create a new column with condition
matches=matches.withColumn("rained",when(col("method")=="D/L","rain").otherwise(None))

#trim extra spaces from start and end
matches=matches.select([ trim(col(c)).alias(c) if matches.schema[c].dataType.simpleString() == 'string' else col(c)
    for c in matches.columns
])

#change datatype of columns
matches=matches.withColumn("player_id",col("player_id").cast("int"))
matches=matches.withColumn("result_margin",col("result_margin").cast("int"))
matches=matches.withColumn("target_runs",col("target_runs").cast("int"))
matches=matches.withColumn("target_overs",col("target_overs").cast("int"))

#load data to postgreSQL database
matches.write.format("jdbc")\
 .option("url","jdbc:postgresql://localhost:5432/myProject")\
 .option("driver","org.postgresql.Driver")\
 .option("dbtable","matches")\
 .option("user","hadoop")\
 .option("password","password")\
 .mode('append')\
 .save()

inserted_count = matches.count()

with open("/home/hadoop/row_counts/matches_count.txt", "w") as f:
    f.write(str(inserted_count))

#hadoop command to move file from landing location to archives
#subprocess.run(['hdfs','dfs','-mv','/files/matches.csv','/archives'])

print("*****SPARK JOB HAS RUN SUCCESSFULLY.*****")
print("******TRANSFORMATION HAS BEED DONE.******")
print("**MATCHES.CSV FILE COPIED TO ARCHIVES.**")
print("*********MATCHES.CSV HAS LOADED.*********")


