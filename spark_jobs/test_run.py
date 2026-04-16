import sys
import os
sys.path.append("/home/naman/Desktop/Metadata")

from pyspark.sql import SparkSession
from src.metadata_manager.manager import MetadataManager
from spark_jobs.pipeline_engine import PipelineEngine

def main():
    print("Starting Spark Session with Delta Lake...")
    # Using local execution matched to Spark 3.4.1 (which uses Delta 2.4.0)
    spark = SparkSession.builder \
        .appName("PipelineEngineTest") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    print("Initializing MetadataManager...")
    meta = MetadataManager("/home/naman/Desktop/Metadata/config/metadata_config.yaml")
    
    print("Initializing PipelineEngine...")
    engine = PipelineEngine(spark, meta)

    print("Running Pipeline: orders_pipeline...")
    engine.run_pipeline("orders_pipeline")

    print("\n--- Pipeline Run Completed ---")
    print("Let's read back the target Delta table to verify:")
    df = spark.read.format("delta").load("/home/naman/Desktop/Metadata/datasets/clean_orders")
    df.show()

if __name__ == "__main__":
    main()
