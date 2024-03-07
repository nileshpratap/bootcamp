from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import time
import os
import pandas as pd
from cryptography.fernet import Fernet


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

Policies = None
Claims = None

def AnalyzePolicies ():
    pdf = spark.read \
        .format("jdbc") \
        .option("url", postgres_url) \
        .option("dbtable", '"public"."Policies"') \
        .option("user", "admin") \
        .option("password", "T6hDdYfHDsISFUCn7LCcsDxqqwt81qqM") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    cdf = spark.read \
        .format("jdbc") \
        .option("url", postgres_url) \
        .option("dbtable", '"public"."Claims"') \
        .option("user", "admin") \
        .option("password", "T6hDdYfHDsISFUCn7LCcsDxqqwt81qqM") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    # pdf.show()
    # cdf.show()
    joined_df = pdf.join(cdf, pdf["PID"] == cdf["PID"], how='inner')
    # joined_df.show()
    # Group by PID and aggregate to count open and closed claims
    grouped_df = joined_df.groupBy(pdf["PID"]).agg(
        F.first(pdf['StartDate']).alias('StartDate'),
        F.first(pdf['EndDate']).alias('EndDate'),
        F.first(pdf['PAmount']).alias('PAmount'),
        F.first(pdf['Status']).alias('Status'),
        F.sum(F.when(cdf["Status"] == "Under Review", 1).otherwise(0)).alias("OpenClaims"),
        F.sum(F.when(cdf["Status"] == "Approved", 1).otherwise(0)).alias("ApprovedClaims"),
        F.sum(F.when(cdf["Status"] == "Declined", 1).otherwise(0)).alias("DeclinedClaims")
    )
    # Show the result
    return grouped_df

def encrypt(df):
    # Generate a key for encryption (this should be kept secret and not hardcoded)
    key = Fernet.generate_key()
    cipher_suite = Fernet(key)

    # Encrypt the 'PID' and 'PAmount' columns
    df['PID'] = df['PID'].apply(lambda x: cipher_suite.encrypt(str(x).encode()))
    df['PAmount'] = df['PAmount'].apply(lambda x: cipher_suite.encrypt(str(x).encode()))
    
    return df

while True:
    df = AnalyzePolicies()
    pdf = df.toPandas()
    pdf = encrypt(pdf)
    pdf.to_csv('Encrypted_Policies_Analysis.csv', index=False)
    time.sleep(60*60*24*7)  # Sleep for 1 week


# # Stop the Spark session
spark.stop()