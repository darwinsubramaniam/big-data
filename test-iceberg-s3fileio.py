#!/usr/bin/env python3

import os
import sys
from pyspark.sql import SparkSession
import traceback

# Set AWS region environment variable for AWS SDK v2
os.environ['AWS_REGION'] = 'us-east-1'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

def test_iceberg_s3fileio():
    """Test Iceberg integration using S3FileIO with AWS SDK v2 only"""
    try:
        print("🔧 Creating SparkSession with S3FileIO...")
        
        spark = SparkSession.builder \
            .appName("IcebergS3FileIOTest") \
            .master("spark://localhost:7077") \
            .config("spark.jars", "./jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar,./jars/iceberg-aws-1.9.2.jar,./jars/aws-sdk-java-2.28.29-bundle.jar") \
            .config("spark.driver.extraClassPath", "./jars/*") \
            .config("spark.executor.extraClassPath", "/opt/spark/jars/*") \
            .config("spark.driver.extraJavaOptions", "-Daws.region=us-east-1") \
            .config("spark.executor.extraJavaOptions", "-Daws.region=us-east-1") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "rest") \
            .config("spark.sql.catalog.iceberg.uri", "http://localhost:8181") \
            .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse") \
            .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
            .config("spark.sql.catalog.iceberg.s3.access-key-id", "admin") \
            .config("spark.sql.catalog.iceberg.s3.secret-access-key", "password") \
            .config("spark.sql.catalog.iceberg.s3.region", "us-east-1") \
            .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
            .config("spark.sql.catalog.iceberg.s3.staging-dir", "/tmp") \
            .getOrCreate()

        print("✅ SparkSession created successfully")
        print(f"✅ Spark version: {spark.version}")
        
        # Test catalog connectivity
        print("\n📋 Available catalogs:")
        spark.sql("SHOW CATALOGS").show()
        
        # Drop namespace to clear any cached s3a:// metadata
        print("\n🗑️ Dropping namespace 'darwin' to clear metadata...")
        try:
            spark.sql("DROP NAMESPACE IF EXISTS iceberg.darwin CASCADE")
            print("✅ Namespace dropped")
        except:
            print("⚠️ Namespace didn't exist or couldn't be dropped")
        
        # Test namespace creation
        print("\n📁 Creating namespace 'darwin' if not exists...")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.darwin")
        print("✅ Namespace created/verified")
        
        # List namespaces
        print("\n📂 Available namespaces:")
        spark.sql("SHOW NAMESPACES IN iceberg").show()
        
        # Check existing table (it was successfully created in previous test!)
        table_name = "s3fileio_fresh_test"
        print(f"\n📊 Checking existing table '{table_name}'...")
        
        # List tables to confirm it exists
        print("\n📋 Available tables:")
        spark.sql("SHOW TABLES IN iceberg.darwin").show()
        
        # Query the existing data
        print(f"\n📊 Querying data from {table_name}:")
        result = spark.sql(f"SELECT * FROM iceberg.darwin.{table_name}")
        result.show()
        
        # Test inserting additional data to verify S3FileIO is working
        print("\n📝 Inserting additional test data...")
        spark.sql(f"""
            INSERT INTO iceberg.darwin.{table_name} VALUES
            (4, 'David', 28),
            (5, 'Eve', 32)
        """)
        print("✅ Additional data inserted successfully")
        
        # Query all data
        print("\n📊 Querying all data:")
        spark.sql(f"SELECT * FROM iceberg.darwin.{table_name} ORDER BY id").show()
        
        print("\n🎉 All tests passed! Iceberg S3FileIO integration is working!")
        
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        traceback.print_exc()
    finally:
        try:
            print("\nShutting down Spark session...")
            spark.stop()
        except:
            pass

if __name__ == "__main__":
    test_iceberg_s3fileio()
