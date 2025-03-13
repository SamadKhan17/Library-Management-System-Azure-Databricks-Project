# Databricks notebook source
#Show all scopes in Databricks

scopes = dbutils.secrets.listScopes()
display(scopes)

# COMMAND ----------

#Show all secrets in scope

secrets = dbutils.secrets.list("lms-scope")
display(secrets)

# COMMAND ----------

#Reading secrets from scope

appid = dbutils.secrets.get(scope="lms-scope",key="lms-appid")
service_credential = dbutils.secrets.get(scope="lms-scope",key="lms-secretid")
directoryid = dbutils.secrets.get(scope="lms-scope",key="lms-tenant")

display(appid,service_credential,directoryid)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ######Mounting ADB with ADLS Gen2 (For reading the data)

# COMMAND ----------

# %python
# configs = {"fs.azure.account.auth.type": "OAuth",
#           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#           "fs.azure.account.oauth2.client.id": appid,
#           "fs.azure.account.oauth2.client.secret": service_credential,
#           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directoryid}/oauth2/token"}

# # Optionally, you can add <directory-name> to the source URI of your mount point.
# dbutils.fs.mount(
#   source = "abfss://bronze@lmsstorageaccount24.dfs.core.windows.net/",
#   mount_point = "/mnt/bronze",
#   extra_configs = configs)

# COMMAND ----------

#Show all data in silver

display(dbutils.fs.ls("/mnt/bronze"))

# COMMAND ----------

#Email encryption library installation
%pip install pycryptodome

# COMMAND ----------

#Loading all the required libraries
from pyspark.sql.functions import *

#Email encryption functions
from pyspark.sql.functions import udf
from Crypto.Cipher import AES
import base64
import os

#Library for Delta Table
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Books Table

# COMMAND ----------

#Reading the data

books = spark.read.parquet("/mnt/bronze/books/")

display(books)

# COMMAND ----------

#Data-type of each column

books.printSchema()

# COMMAND ----------

#How many records in books dataset

books.count()

# COMMAND ----------

#Convert book_price column to double

books = books.withColumn('book_price', round(col('book_price')))

# COMMAND ----------

#Descriptive stats of book_price

books.select('book_price').describe().show() 

# COMMAND ----------

#Renaming book_id to BK 

# books = books.withColumnRenamed('book_id','BK')
# books.display(10)

# COMMAND ----------

#Checking for Null values in each column

books.select([sum(col(c).isNull().cast("int")).alias(c) for c in books.columns]).display()

# COMMAND ----------

#Checking how many duplicate rows we have

books.count() - books.distinct().count()  ##total rows - unique rows

# COMMAND ----------

#Checking for duplicate rows for book_id unique column

#Find duplicate book_id values
duplicate_book_ids = books.groupBy("book_id").count().filter(col("count") > 1).select("book_id")

#Join back to original DataFrame to get all rows with duplicate book_id
duplicate_rows = books.join(duplicate_book_ids, on="book_id", how="inner")

#Show duplicate rows
duplicate_rows.display()

# COMMAND ----------

#Verifying the duplicate records by considering one value 

books.filter(col("book_id") == "BK023").display()

# COMMAND ----------

#Removing the duplicates records from book_id column
books = books.dropDuplicates(["book_id"])

#Verifying whether the duplicate rows removed or not
books.filter(col("book_id") == "BK023").display()

# COMMAND ----------

books.count()

#32 duplicate rows have been removed

# COMMAND ----------

#Converting author column values to lowercase

books = books.withColumn('author', lower(col('author')))
books = books.withColumn('publisher', lower(col('publisher')))

books.display() 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Saving books dataset into Delta format under Silver 

# COMMAND ----------

#Saving the data in delta format under silver location

books.write.format("delta").mode("overwrite").save("abfss://silver@lmsstorageaccount24.dfs.core.windows.net/books")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creating external table for the silver location in Unity Catalog
# MAGIC DROP TABLE IF EXISTS `lms-catalog`.silver.books;
# MAGIC
# MAGIC CREATE TABLE `lms-catalog`.silver.books
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@lmsstorageaccount24.dfs.core.windows.net/books';

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Books Copies Table

# COMMAND ----------

display(dbutils.fs.ls('/mnt/bronze/'))

# COMMAND ----------

#Reading the book copies dataset

books_copies = spark.read.parquet('/mnt/bronze/book_copies', header=True, inferSchema=True)

books_copies.display()

# COMMAND ----------

#Data-type of each column

books_copies.printSchema()

# COMMAND ----------

#Total records

books_copies.count()

# COMMAND ----------

#Checking for missing values

books_copies.select([sum(col(c).isNull().cast("int")).alias(c) for c in books_copies.columns]).show()

# COMMAND ----------

#Total duplicate rows we have in our dataset 

books_copies.count() - books_copies.distinct().count()  #total rows - unique rows

# COMMAND ----------

#Checking for duplicate rows for copy_id unique column

#Find duplicate copy_id values
duplicate_copy_ids = books_copies.groupBy("copy_id").count().filter(col("count") > 1).select("copy_id")

#Join back to original DataFrame to get all rows with duplicate book_id
duplicate_rows_copy = books_copies.join(duplicate_copy_ids, on="copy_id", how="inner")

#Show duplicate rows
duplicate_rows_copy.display()

# COMMAND ----------

#Verifying the duplicate rows in copy_id column 

duplicate_rows_copy.filter(col("copy_id") == "CP00281").display()

# COMMAND ----------

#Removing the duplicates observation from copy_id column
books_copies = books_copies.dropDuplicates(["copy_id"])

#Verifying whether the duplicate rows removed or not
books_copies.filter(col("copy_id") == "CP00281").display()

# COMMAND ----------

books_copies.count() #14 duplicates rows removed from copy_id column

# COMMAND ----------

books_copies.show(5)

# COMMAND ----------

#Converting the status column values into lower case

books_copies = books_copies.withColumn("status", lower(col("status")))
books_copies.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Saving books copies dataset into Delta format under Silver 

# COMMAND ----------

#Saving the data in delta format under silver location

books_copies.write.format("delta").mode("overwrite").save("abfss://silver@lmsstorageaccount24.dfs.core.windows.net/books_copies")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creating external table for the silver location in Unity Catalog
# MAGIC DROP TABLE IF EXISTS `lms-catalog`.silver.books_copies;
# MAGIC
# MAGIC CREATE TABLE `lms-catalog`.silver.books_copies
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@lmsstorageaccount24.dfs.core.windows.net/books_copies';

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Students Table

# COMMAND ----------

display(dbutils.fs.ls('/mnt/bronze/'))

# COMMAND ----------

#Reading the students dataset

students = spark.read.parquet('/mnt/bronze/students')

students.display()

# COMMAND ----------

#Data-types of students columns

students.printSchema()

# COMMAND ----------

#Convert student_year column to int

students = students.withColumn("student_year", col("student_year").cast("int"))

# COMMAND ----------

#Total rows

students.count()

# COMMAND ----------

#Checking for missing values in students table

students.select([sum(col(c).isNull().cast("int")).alias(c) for c in students.columns]).show()

# COMMAND ----------

#Checking how many duplicate rows we have

students.count() - students.distinct().count()  #total rows - unique rows

# COMMAND ----------

#Checking for duplicate rows for students_id unique column

#Find duplicate student_id values
duplicate_students_ids = students.groupBy("student_id").count().filter(col("count") > 1).select("student_id")

#Join back to original DataFrame to get all rows with duplicate student_id
duplicate_rows_students = students.join(duplicate_students_ids, on="student_id", how="inner")

#Show duplicate rows
display(duplicate_rows_students)

# COMMAND ----------

#Verifying the duplicate rows in students_id column 

duplicate_rows_students.filter(col("student_id") == "S20EC012").display()

# COMMAND ----------

#Removing the duplicates observation from students_id column
students = students.dropDuplicates(["student_id"])

#Verifying whether the duplicate rows removed or not
students.filter(col("student_id") == "S20EC012").display()

# COMMAND ----------

students.count() #14 duplicates rows removed from student_id column

# COMMAND ----------

#Converting first_name and last_name columns values to lower

students = students.withColumn('first_name', lower(col('first_name')))
students = students.withColumn('last_name', lower(col('last_name')))

students.select('first_name','last_name').show(5) 

# COMMAND ----------

#unique (distinct) all values of section column

students.select('section').distinct().display()

# COMMAND ----------

#Update section column values
students = students.withColumn(
                "section",when(col("section") == "A#", "A")\
               .when(col("section") == "B#", "B")\
               .otherwise(col("section"))
)

#Show updated DataFrame
students.select('section').distinct().display()

#Now only 2 values are there, A and B. No more values like A# and B# etc.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #####Encrypting the email column

# COMMAND ----------

#Creating AES Encryption Function
def encrypt_email(email: str, key: str) -> str:
    if email is None:
        return None
    key = key.ljust(32)[:32].encode("utf-8")  #Ensure key is 32 bytes
    cipher = AES.new(key, AES.MODE_EAX)
    ciphertext, tag = cipher.encrypt_and_digest(email.encode("utf-8"))
    return base64.b64encode(cipher.nonce + tag + ciphertext).decode("utf-8")

#Define the encryption key securely (Using Databricks Secrets instead of hardcoding)
encryption_key = dbutils.secrets.get(scope="lms-scope", key="encryptionkey")

#Register UDF
encrypt_udf = udf(lambda email: encrypt_email(email, encryption_key))

#Apply Encryption to the DataFrame
students = students.withColumn("email_encrypted", encrypt_udf(students.email))

#Drop original email column for security
students = students.drop("email")

#Show Encrypted Emails
students.show(5)

# COMMAND ----------

#Defining the Decryption Function

def decrypt_email(enc_email: str, key: str) -> str:
    if enc_email is None:
        return None
    key = key.ljust(32)[:32].encode("utf-8")
    enc_bytes = base64.b64decode(enc_email)
    nonce, tag, ciphertext = enc_bytes[:16], enc_bytes[16:32], enc_bytes[32:]
    cipher = AES.new(key, AES.MODE_EAX, nonce=nonce)
    return cipher.decrypt_and_verify(ciphertext, tag).decode("utf-8")

#Register UDF
decrypt_udf = udf(lambda enc_email: decrypt_email(enc_email, encryption_key)) #decrypting using the encryption_key we saved in Key Vault.

#Apply Decryption to the DataFrame
students = students.withColumn("email_decrypted", decrypt_udf(students.email_encrypted))

#Show Decrypted Emails
students.select("email_encrypted", "email_decrypted").show(5)

# COMMAND ----------

#Dropping the decrypted email column

students = students.drop("email_decrypted")
students.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Saving students dataset into Delta format under Silver 

# COMMAND ----------

#Saving the data in delta format under silver location

students.write.format("delta").mode("overwrite").save("abfss://silver@lmsstorageaccount24.dfs.core.windows.net/students")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creating external table for the silver location in Unity Catalog
# MAGIC DROP TABLE IF EXISTS `lms-catalog`.silver.students;
# MAGIC
# MAGIC CREATE TABLE `lms-catalog`.silver.students
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@lmsstorageaccount24.dfs.core.windows.net/students';

# COMMAND ----------

# MAGIC %md
# MAGIC #Transactions Table

# COMMAND ----------

display(dbutils.fs.ls('/mnt/bronze/transactions'))

# COMMAND ----------

fileQuarter = dbutils.widgets.get("fileQuarter")

# COMMAND ----------

file_name = f"transactions_{fileQuarter}.parquet"

# COMMAND ----------

file_path = f"/mnt/bronze/transactions/{file_name}"

# COMMAND ----------

transactions = spark.read.parquet(file_path)

# COMMAND ----------

# parquet_files = dbutils.fs.ls('/mnt/bronze/transactions/')

# #Filter for parquet files and sort by name to get the latest file
# latest_file = sorted([f.path for f in parquet_files if f.path.endswith('.parquet')])[-1]


# COMMAND ----------

#Read the latest parquet file
# transactions = spark.read.parquet(latest_file)
transactions.display()

# COMMAND ----------

#Data-type of each column

transactions.printSchema()

# COMMAND ----------

#Converting columns data-type

transactions = transactions.withColumn("year", col("year").cast("int")) \
                   .withColumn("quarter", col("quarter").cast("int")) \
                   .withColumn("fine_amount", col("fine_amount").cast("double"))

#Converting date columns to date data-type
transactions = transactions.withColumn("issue_date", to_date(col("issue_date"), "dd-MM-yyyy")) \
                   .withColumn("return_date", to_date(col("return_date"), "dd-MM-yyyy")) \
                   .withColumn("due_date", to_date(col("due_date"), "dd-MM-yyyy"))
        
transactions.printSchema()

# COMMAND ----------

#Total rows 

transactions.count()

# COMMAND ----------


#Descriptive stats on fine_amount column

transactions.describe("fine_amount").show()

# COMMAND ----------

#Checking for missing values

transactions.select([sum(col(c).isNull().cast("int")).alias(c) for c in transactions.columns]).display()

#return_date reflects how many students did not returned the book taken from library,
#payment_date reflects how many students did not paid the fine amount

# COMMAND ----------

#Checking how many duplicate rows we have

transactions.count() - transactions.distinct().count()  #total rows - unique rows

# COMMAND ----------

#Checking for duplicate rows for transaction_id unique column

#Find duplicate transaction_id values
duplicate_transaction_ids = transactions.groupBy("transaction_id").count().filter(col("count") > 1).select("transaction_id")

#Join back to original DataFrame to get all rows with duplicate transaction_id
duplicate_rows_transaction_ids = transactions.join(duplicate_transaction_ids, on="transaction_id", how="inner")

#Show duplicate rows
display(duplicate_rows_transaction_ids)

#NO DUPLICATES RECORDS.

# COMMAND ----------

transactions.display()

#Need to remove _ values from transaction_id column

# COMMAND ----------

#Using Regular Expression to replace underscore with empty string

#Remove underscores from transaction_id column
transactions = transactions.withColumn("transaction_id", regexp_replace(col("transaction_id"), "_", ""))

#Show updated DataFrame
transactions.display()

# COMMAND ----------

#Check for dates starting from DD- instead of YYYY-

transactions.select('issue_date','due_date','return_date','payment_date').show(5)

#ALL COLUMN FORMAT IS NOT IN CORRECT FORMAT -> (YYYY-MM-DD)

# COMMAND ----------

#Convert payment_date (which has time) into proper date format (YYYY-MM-DD)

transactions = transactions.withColumn("payment_date",to_date(to_timestamp(col("payment_date"), "dd-MM-yyyy HH:mm")))

#Converting all other date colunns to proper date format YYYY-MM-DD
transactions = transactions.withColumn("issue_date", to_date(col("issue_date"), "yyyy-MM-dd")) \
                   .withColumn("return_date", to_date(col("return_date"), "yyyy-MM-dd")) \
                   .withColumn("due_date", to_date(col("due_date"), "yyyy-MM-dd"))

#Show updated values
transactions.select('issue_date', 'due_date','return_date', 'payment_date').show(5)

# COMMAND ----------

#Regular expression to match dates starting with DD-MM-YYYY (01-31 at the start)
date_pattern = r"^[0-3][0-9]-[0-1][0-9]-[1-2][0-9]{3}$"

#*Check if any rows have DD-MM-YYYY format instead of YYYY-MM-DD*
incorrect_dates = transactions.filter(
    col("issue_date").rlike(date_pattern) | 
    col("due_date").rlike(date_pattern) | 
    col("return_date").rlike(date_pattern) |
    col("payment_date").rlike(date_pattern))

#Show incorrect format dates
incorrect_dates.select("issue_date", "due_date", "return_date", "payment_date").display(truncate=False)

# COMMAND ----------

#Filter rows where book_id starts with 'bk' (lowercase) only

bk_lowercase_check = transactions.filter(
    col("book_id").rlike("^bk[0-9]+$")  #Matches bk001, bk105, etc.
    ).filter(~col("book_id").rlike("^BK[0-9]+$"))  #Excludes uppercase BK

#Show results
bk_lowercase_check.display()

# COMMAND ----------

#Filter rows where copy_id starts with 'cp' instead of 'CP'
cp_lowercase_check = transactions.filter(col("copy_id").rlike("^cp[0-9]+$"))

#Show results
cp_lowercase_check.select('copy_id').display()

# COMMAND ----------

#Convert book_id to uppercase
transactions = transactions.withColumn("book_id", upper(col("book_id")))

#Convert copy_id to uppercase
transactions = transactions.withColumn("copy_id", upper(col("copy_id")))

# COMMAND ----------

#Filter rows where book_id starts with 'bk' (lowercase) only

bk_lowercase_check = transactions.filter(
    col("book_id").rlike("^bk[0-9]+$")  #Matches bk001, bk105, etc.
    ).filter(~col("book_id").rlike("^BK[0-9]+$"))  #Excludes uppercase BK

#Show results
bk_lowercase_check.display()

#NO LOWER CASE VALUES IS PRESENT NOW IN book_id COLUMN.

# COMMAND ----------

#Filter rows where copy_id starts with 'cp' instead of 'CP'
cp_lowercase_check = transactions.filter(col("copy_id").rlike("^cp[0-9]+$"))

#Show results
cp_lowercase_check.select('copy_id').display() 

#NO LOWER CASE VALUES IS PRESENT NOW IN copy_id COLUMN.

# COMMAND ----------

#Upper cases values are present we want all values to be lower case

transactions.select('initial_status').distinct().show()
transactions.select('final_status').distinct().show()
transactions.select('payment_status').distinct().show()

# COMMAND ----------

#Convert initial_status and final_status to lowercase
transactions = transactions.withColumn("initial_status", lower(col("initial_status"))) \
                                           .withColumn("final_status", lower(col("final_status")))\
                                            .withColumn("payment_status", lower(col("payment_status")))

# COMMAND ----------

transactions.select('initial_status').distinct().show()
transactions.select('final_status').distinct().show()
transactions.select('payment_status').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #####Saving the data in Silver container
# MAGIC
# MAGIC ######*This code will save the transaction data into Silver Container in ADLS Gen2 in 1st run of ADF Pipeline, then append the new data in 2nd run of ADF Pipeline.*

# COMMAND ----------

#Define the path to the Delta table in the silver container
silver_table_path = 'abfss://silver@lmsstorageaccount24.dfs.core.windows.net/transactions'

#Check if the DataFrame is empty
if transactions.isEmpty():
    print("No new data to append to the Delta table.")
else:
    #Check if the Delta table already exists
    try:
        #Try to read the existing Delta table
        existing_df = spark.read.format("delta").load(silver_table_path)
        
        #If it exists, append the new data
        transactions.write.format("delta").mode("append").save(silver_table_path)
        print(f"Appended new data to the existing Delta table at: {silver_table_path}")
    except AnalysisException:
        #If the table does not exist, create it
        transactions.write.format("delta").mode("overwrite").save(silver_table_path)
        print(f"Created new Delta table at: {silver_table_path}")  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #####Creating or Appending the Delta Table in Silver Schema according to the requirement

# COMMAND ----------

#Define the Delta table name
silver_table_name = "`lms-catalog`.silver.transactions"

#Check if the Delta table already exists
try:
    #Try to read the existing Delta table
    existing_df = spark.read.format("delta").table(silver_table_name)
    
    #If it exists, append the new data
    transactions.write.format("delta").mode("append").saveAsTable(silver_table_name)
    print(f"Appended new data to the existing Delta table: {silver_table_name}")
except AnalysisException:
    #If the table does not exist, create it
    transactions.write.format("delta").mode("overwrite").saveAsTable(silver_table_name)
    print(f"Created new Delta table: {silver_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Verifying if the data is loaded in Silver Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from `lms-catalog`.silver.transactions