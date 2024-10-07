import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Function to extract time in minutes from ISO format
def extract_time_in_minutes(time_col):
    try:
        return (
            F.coalesce(F.regexp_extract(time_col, "PT(\d+)H(\d+)M", 1).cast('int'), F.lit(0)) * 60 +
            F.coalesce(F.regexp_extract(time_col, "PT(\d+)H(\d+)M", 2).cast('int'), F.lit(0)) +
            F.coalesce(F.regexp_extract(time_col, "PT(\d+)H(?!\d+M)", 1).cast('int'), F.lit(0)) * 60 +
            F.coalesce(F.regexp_extract(time_col, "PT(\d+)M(?!\d+H)", 1).cast('int'), F.lit(0))
        )
    except Exception as e:
        print(f"Error extracting time in minutes: {e}")
        return F.lit(0)

# Function to analyze recipes
def analyze_recipes(spark, parquet_path):
    try:
        # Read the Parquet file
        df_clean = spark.read.parquet(parquet_path)
        
        # Check if DataFrame is empty
        if df_clean.rdd.isEmpty():
            print("The DataFrame is empty.")
            return

        # Filter recipes containing 'beef' in ingredients
        df_clean = df_clean.filter(F.lower(F.col('ingredients')).contains('beef'))

        # Calculate cookTimeMinutes
        df_beef = df_clean.withColumn('cookTimeMinutes', extract_time_in_minutes(col("cookTime")))
        
        # Calculate prepTimeMinutes
        df_beef = df_beef.withColumn('prepTimeMinutes', extract_time_in_minutes(col("prepTime")))

        # Calculate total cook time
        df_beef = df_beef.withColumn("total_cook_time", col("cookTimeMinutes") + col("prepTimeMinutes"))

        # Determine difficulty level based on total cook time
        df_beef = df_beef.withColumn(
            "difficulty",
            expr(
                "CASE WHEN total_cook_time < 30 THEN 'easy' "
                "WHEN total_cook_time BETWEEN 30 AND 60 THEN 'medium' "
                "ELSE 'hard' END"
            )
        )

        # Calculate average cooking time duration per difficulty level
        df_avg_cook_time = df_beef.groupBy("difficulty").agg(round(avg("total_cook_time")).alias("avg_total_cooking_time"))
        
        # Print the result DataFrame
        df_avg_cook_time.show()
        return df_avg_cook_time

    except Exception as e:
        print(f"Error in analyze_recipes function: {e}")

# Please define path for the parquet_path
parquet_path = ""

# Initialize Spark session
try:
    spark = SparkSession.builder.appName("Analyze Recipes").getOrCreate()
    spark.conf.set("spark.hadoop.io.native.lib.available", "false")
except Exception as e:
    print(f"Error initializing Spark session: {e}")

# Call the function to analyze recipes
result_df = analyze_recipes(spark, parquet_path)

# If result_df is not None, write the result DataFrame to a CSV file, please add csv_output_path
if result_df:
    try:
        csv_output_path = ""
        result_df.write.csv(csv_output_path, header=True, mode="overwrite")
    except Exception as e:
        print(f"Error writing DataFrame to CSV: {e}")
else:
    print("No result DataFrame to write to CSV.")
