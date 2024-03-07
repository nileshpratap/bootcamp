from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
    "Customers": '"public"."Customers"',
    "Admins": '"public"."Admins"',
    "Policies": '"public"."Policies"',
    "Claims": '"public"."Claims"',

}


# prints DB schema
# df.printSchema()

# Show the only updated_on column of df
# df.select("updated_on").show(truncate=False)

# show all the df rows
# df..show()


def getupdateddata ():
    for tkeyword, tname in table_names.items():
        options = {
            "url": postgres_url,
            "dbtable": tname,
            "driver": postgres_props["driver"],
            "user": postgres_props["user"],
            "password": postgres_props["password"],
        }
        df = spark.read \
            .format("jdbc") \
            .option("url", options["url"]) \
            .option("dbtable", tname) \
            .option("user", options["user"]) \
            .option("password", options["password"]) \
            .option("driver", options["driver"]) \
            .load()
        # Get the current date
        current_date = datetime.now().date()

        # Calculate the date before seven days
        date_before_seven_days = current_date - timedelta(days=7)

        # Use the filter or where method to get rows updated in last seven days
        current_date_timestamp = datetime.combine(current_date + timedelta(days=1), datetime.min.time())
        date_before_seven_days_timestamp = datetime.combine(date_before_seven_days, datetime.min.time())
        filtered_rows = df.filter(
            (col("updated_on") >= date_before_seven_days_timestamp) & (col("updated_on") <= current_date_timestamp))

        # saving data to csv
        # Convert Spark DataFrame to Pandas DataFrame
        pandas_df = filtered_rows.toPandas()

        csv_file_path = f"updated_{tkeyword} {date_before_seven_days} to {current_date}.csv"
        csv_file_path = f"updated_{tkeyword} {date_before_seven_days} to {current_date}.csv"

        if os.path.exists(csv_file_path):
            os.remove(csv_file_path)

        # Save the Pandas DataFrame to CSV

        pandas_df.to_csv(csv_file_path, index=False)

        # Insert an empty row at the end
        with open(csv_file_path, "a") as csv_file:
            csv_file.write("\n")


        # saving data to pyarrow
        parquet_file_path = f"updated_{tkeyword} {date_before_seven_days} to {current_date}.parquet"
        if os.path.exists(parquet_file_path):
            os.remove(parquet_file_path)

        pandas_df.to_parquet(parquet_file_path, engine='pyarrow')


# while True:
    getupdateddata()
    # Introduce a short delay to avoid high CPU usage
    # time.sleep(60*60*24*7)  # Sleep for 1 week
    # time.sleep(30)  # Sleep for 30seconds
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} // executing the routine" )


# # Stop the Spark session
spark.stop()
