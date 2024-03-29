import pandas as pd
import re
import logging
import os
from datetime import datetime, timedelta
from datetime import datetime
from logging.handlers import RotatingFileHandler


def checkUIDValidity(column, value, UID):
        if isinstance(value, int):
            pass
        else:
            logging.error(f"{column} for Customer with UID:{UID} is not an Int")

        uidstr = str(value)
        if len(uidstr) == 16 and uidstr.isdigit():
            pass
        else:
            logging.error(f"{column} for Customer with UID:{UID} is not 16 digit Number")


def checkNameValidity(column, value, UID):
        if isinstance(value, str):
            pass
        else:
            logging.error(f"{column} for Customer with UID:{UID} is not a String")

        if value.isalpha():
            pass
        else:
            logging.error(f"{column} for Customer with UID:{UID} should contain only alphabets")



def checkEmailValidity(column, value, UID):
    if isinstance(value, str):
        pass
    else:
        logging.error(f"{column} for Customer with UID:{UID} is not a String")

    if re.match(r"[^@]+@[^@]+\.[^@]+", value):
        pass
    else:
        logging.error(f"{column} for Customer with UID:{UID} is not a valid email address")


def checkPasswordValidity(column, value, UID):
    if isinstance(value, str):
        pass
    else:
        logging.error(f"{column} for Customer with UID:{UID} is not a String")
    if len(value) == 0:
        logging.error(f"{column} for Customer with UID:{UID} is empty")
    else:
        pass


def checkHealthConditionValidity(column, value, UID):
    if isinstance(value, int):
        pass
    else:
        logging.error(f"{column} for Customer with UID:{UID} is not a Int")
    if value < 0 or value > 10:
        logging.error(f"{column} for Customer with UID:{UID} is not between 1 and 10")
    else:
        pass


def checkDOBValidity(column, value, UID):
    if isinstance(value, str):
        pass
    else:
        logging.error(f"{column} for Customer with UID:{UID} is not a Date")
    try:
        datetime_object = datetime.strptime(value, '%Y-%m-%d')

    except ValueError as e:
        logging.error(f"{column} for Customer with UID:{UID} invalid, Error: {e}")


def checkPIDsValidity(column, value, UID):
    if isinstance(value, str) and len(value)>=2 and value[0]=='[' and value[-1]==']':
        pass
    else:
        logging.error(f"{column} for Customer with UID:{UID} is not an array")



log_file_path = 'Customers_Data_Quality_Issues.log'
if os.path.exists(log_file_path):
    os.remove(log_file_path)
# Configure the logging module
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        RotatingFileHandler(log_file_path, maxBytes=1048576, backupCount=5)  # 1 MB max size, keep 5 backups
    ]
)

# File paths for CSV and Parquet files on the desktop
# Get the current date
current_date = datetime.now().date()

# Calculate the date before seven days
date_before_seven_days = current_date - timedelta(days=7)
csv_file_path = f"updated_Customers {date_before_seven_days} to {current_date}.csv"

# Load CSV data into a DataFrame
df = pd.read_csv(csv_file_path)

# Data quality checks for each row
for index, row in df.iterrows():
    UID = None
    if index == 0:
        for column, value in row.items():
            if column == "UID":
                UID = value
                checkUIDValidity(column, value, UID)
            elif column == "Name":
                checkNameValidity(column, value, UID)
            elif column == "Email":
                checkEmailValidity(column, value, UID)
            elif column == "Password":
                checkPasswordValidity(column, value, UID)
            elif column == "HealthCondition":
                checkHealthConditionValidity(column, value, UID)
            elif column == "DOB":
                checkDOBValidity(column, value, UID)
            elif column == "PIDs":
                checkPIDsValidity(column, value, UID)

            else:
                pass

def count_lines_in_file(file_path):
    try:
        with open(file_path, 'r') as file:
            line_count = sum(1 for line in file)
        return line_count
    except FileNotFoundError:
        return None  # Log file not found


line_count = count_lines_in_file(log_file_path)

if line_count != 0:
    print("Some data Customers is not in good quality")
else:
    print("All the Customers data is in good quality")