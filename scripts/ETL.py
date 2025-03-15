import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, avg, broadcast, count, when, regexp_extract
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, LongType

# this is custom configuration file script that contains the file paths, special variables required for the script to run
from configuration import *


# Initialize Logging

def check_nulls(df, df_name):
    print(f"Checking null values in {df_name} DataFrame...")
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0]
    for column, null_count in null_counts.asDict().items():
        if null_count > 0:
            print(f"{df_name} contains {null_count} null values in column {column}. Dropping nulls.")
            logging.warning(f"{df_name} contains {null_count} null values in column {column}. Dropping nulls.")
    return df.dropna()


def validate_schema(df, expected_schema, df_name):
    print("Validating schema for {} DataFrame...".format(df_name))
    if df.schema != expected_schema:
        logging.error(f"Schema Mismatch for {df_name}! Expected: {expected_schema}, Found: {df.schema}")
        raise ValueError(f"Schema Mismatch for {df_name}! Check logs for details.")
    logging.info(f"Schema validation passed for {df_name}.")
    print("Schema validation passed for {} DataFrame.".format(df_name))

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "etl_log.txt")
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Spark Session
spark = (SparkSession.builder.appName("MovieRatingsETL")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.sql.adaptive.skewJoin.enabled", "true")
         .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
         .getOrCreate())
logging.info("Spark session initialized successfully.")

csv_output_dir = "output/csv_results"
os.makedirs(csv_output_dir, exist_ok=True)

# Define schemas
movies_schema = StructType([
    StructField("MovieID", IntegerType(), True),
    StructField("Title", StringType(), True),
    StructField("Genres", StringType(), True)
])
ratings_schema = StructType([
    StructField("UserID", IntegerType(), True),
    StructField("MovieID", IntegerType(), True),
    StructField("Rating", FloatType(), True),
    StructField("Timestamp", LongType(), True)
])
users_schema = StructType([
    StructField("UserID", IntegerType(), True),
    StructField("Gender", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Occupation", IntegerType(), True),
    StructField("ZipCode", StringType(), True)
])

# Load datasets
logging.info("Loading datasets...")
movies_df = spark.read.option("delimiter", "::").schema(movies_schema).csv(movies_path)
ratings_df = spark.read.option("delimiter", "::").schema(ratings_schema).csv(ratings_path)
users_df = spark.read.option("delimiter", "::").schema(users_schema).csv(users_path)
logging.info("Datasets loaded successfully.")

#Print df
print(movies_df.show(), 'Movies Count:', movies_df.count())
print(ratings_df.show(), 'Rating Count:', ratings_df.count())
print(users_df.show(), 'Users Count:', users_df.count())

load_type = "incremental"  # Change to 'full' if full load is required


validate_schema(movies_df, movies_schema, "Movies")
validate_schema(ratings_df, ratings_schema, "Ratings")
validate_schema(users_df, users_schema, "Users")

movies_df = check_nulls(movies_df, "Movies")
ratings_df = check_nulls(ratings_df, "Ratings")
users_df = check_nulls(users_df, "Users")

# Filter movies released after 1989
print("Filtering movies released after 1989...")
print("Total movies before filtering: ", movies_df.count(), "\n")
movies_df = movies_df.withColumn("Year", regexp_extract(col("Title"), r"\((\d{4})\)", 1).cast("int"))
print("Movies with year: ", movies_df.show())
movies_df = movies_df.filter(col("Year") > 1989)
print("Filtered movies released after 1989: ", movies_df.count())
logging.info("Filtered movies released after 1989.")



movies_df = movies_df.dropDuplicates(["MovieID"])
ratings_df = ratings_df.dropDuplicates(["UserID", "MovieID"])
users_df = users_df.dropDuplicates(["UserID"])
logging.info("Removed duplicate entries from datasets.")
print("Removed duplicate entries from datasets.")
print("Movies: ", movies_df.count())
print("Ratings: ", ratings_df.count())
print("Users: ", users_df.count())


users_df = users_df.filter((col("Age") >= 18) & (col("Age") <= 49))
logging.info("Filtered users aged 18-49.")

ratings_df = ratings_df.filter((col("Rating") >= 1.0) & (col("Rating") <= 5.0))
logging.info("Filtered ratings between 1.0 and 5.0.")

movies_df = movies_df.filter(col("Genres").isNotNull() & (col("Genres") != ""))
logging.info("Ensured Genre column is not empty.")

# Cache DataFrames after filtering to optimize performance
movies_df.cache()
ratings_df.cache()
users_df.cache()

movies_df = movies_df.withColumn("Genre", explode(split(col("Genres"), "\\|"))).drop("Genres")
ratings_users_df = ratings_df.join(broadcast(users_df), "UserID", "inner")
movies_ratings_df = ratings_users_df.join(movies_df, "MovieID", "inner")
result_df = movies_ratings_df.groupBy("Year", "Genre").agg(avg("Rating").alias("AvgRating"))
result_df = result_df.repartition("Year")
logging.info("Computed average ratings per genre and year.")

print(result_df.show())

current_date = datetime.now().strftime("%Y-%m-%d")
csv_output_path = os.path.join(csv_output_dir, f"output_{current_date}.csv")
result_df.write.option("header", "true").mode("overwrite").csv(csv_output_path)
logging.info(f"ETL Job Completed Successfully! Output saved at: {csv_output_path}")

spark.stop()
logging.info("Spark session stopped.")
