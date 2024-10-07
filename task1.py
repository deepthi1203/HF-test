import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

def process_recipes(input_path, output_path):
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("Process Recipes").getOrCreate()
        spark.conf.set("spark.hadoop.io.native.lib.available", "false")
        
        # Check if input path exists
        if not os.path.exists(input_path):
            print(f"Path does not exist: {input_path}")
            return
        
        # Get list of JSON files in the input directory
        json_paths = [os.path.join(input_path, filename) for filename in os.listdir(input_path) if filename.endswith('.json')]
        
        if not json_paths:
            print("No JSON files found in the input directory.")
            return
        
        # Read JSON files into a DataFrame
        df = spark.read.json(json_paths)
        
        # Drop rows with any null values
        df_clean = df.dropna()
        
        # Convert datePublished column to date type
        df_clean = df_clean.withColumn("datePublished", to_date(col("datePublished"), "yyyy-MM-dd"))
        
        # Print the cleaned DataFrame
        df_clean.show()
        # return df_clean

        # Write the cleaned DataFrame to S3 in Parquet format
        df_clean.write.mode('append').parquet(output_path)
        
    except Exception as e:
        print(f"Error processing recipes: {e}")
    finally:
        # Stop the Spark session
        spark.stop()

# Please define input and output paths
input_path = ""
output_path = ""

# Call the function
process_recipes(input_path, output_path)
