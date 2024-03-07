from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import time
import os
import pandas as pd


# Create a Spark session
spark = SparkSession.builder \
    .appName("IncrementalUpdate") \
    .config('spark.jars', "postgresql-42.7.2.jar") \
    .getOrCreate()
# print(spark)


# Define the PostgreSQL connection URL
postgres_url = "jdbc:postgresql://dpg-cndhkpf79t8c738e3q0g-a.singapore-postgres.render.com/hcmsdb"

# Define PostgreSQL connection properties
postgres_props = {
    "user": "admin",
    "password": "T6hDdYfHDsISFUCn7LCcsDxqqwt81qqM",
    "driver": 'org.postgresql.Driver',
}

# Define the table name
table_names = {
    "Policies": '"public"."Policies"',
    "Claims": '"public"."Claims"',
}

Claims = None

def AnalyzePolicies ():
    cdf = spark.read \
        .format("jdbc") \
        .option("url", postgres_url) \
        .option("dbtable", '"public"."Claims"') \
        .option("user", "admin") \
        .option("password", "T6hDdYfHDsISFUCn7LCcsDxqqwt81qqM") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    # cdf.show()
    pandasDF = cdf.toPandas()
    # approved, under review, closed
    totalClaims = {'Approved':0, "Under Review":0, "Declined": 0}
    for index, row in pandasDF.iterrows():
        status = row['Status']
        
        if status == 'Under Review':
            totalClaims["Under Review"] += 1

        elif status == 'Approved':
            totalClaims["Approved"] += 1

        elif status == 'Declined':
            totalClaims['Declined'] += 1
    
    toStore =  pd.DataFrame([totalClaims])
    toStore.to_csv('total_claims.csv')
    print(toStore, "HI")
    



while True:
    AnalyzePolicies()
    time.sleep(60*60*24*7)  # Sleep for 1 week


# # Stop the Spark session
spark.stop()