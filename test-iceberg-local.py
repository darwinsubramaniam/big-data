#!/usr/bin/env python3

import os
import sys
from pyspark.sql import SparkSession
import traceback

def test_iceberg_local():
    """Test Iceberg integration using local Spark mode to avoid executor classpath issues"""
    try:
        print("🔧 Creating SparkSession in local mode...")
        
        spark = SparkSession.builder \
            .appName("IcebergLocalTest") \
            .master("local[*]") \
            .config("spark.jars", "./jars/aws-java-sdk-bundle-1.12.262.jar,./jars/hadoop-aws-3.3.4.jar,./jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar,./jars/iceberg-aws-1.9.2.jar,./jars/aws-sdk-java-2.28.29-bundle.jar") \
            .config("spark.driver.extraClassPath", "./jars/*") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "rest") \
            .config("spark.sql.catalog.iceberg.uri", "http://localhost:8181") \
            .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse") \
            .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
            .config("spark.sql.catalog.iceberg.s3.endpoint", "http://localhost:9000") \
            .config("spark.sql.catalog.iceberg.s3.access-key-id", "admin") \
            .config("spark.sql.catalog.iceberg.s3.secret-access-key", "password") \
            .config("spark.sql.catalog.iceberg.s3.region", "us-east-1") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "password") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()

        print("✅ SparkSession created successfully")
        
        # Test catalog connectivity
        print("\n📋 Available catalogs:")
        spark.sql("SHOW CATALOGS").show()
        
        # Test namespace creation
        print("\n📁 Creating namespace 'darwin' if not exists...")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.darwin")
        print("✅ Namespace created/verified")
        
        # List namespaces
        print("\n📂 Available namespaces:")
        spark.sql("SHOW NAMESPACES IN iceberg").show()
        
        # Drop existing table if it exists to avoid schema conflicts
        print("\n🗑️ Dropping existing table if it exists...")
        spark.sql("DROP TABLE IF EXISTS iceberg.darwin.local_test_table")
        
        # Create test table
        print("\n🔨 Creating test table...")
        spark.sql("""
            CREATE TABLE iceberg.darwin.local_test_table (
                id BIGINT,
                name STRING,
                age INT
            ) USING ICEBERG
        """)
        print("✅ Table created successfully")
        
        # Insert test data
        print("\n📝 Inserting test data...")
        spark.sql("""
            INSERT INTO iceberg.darwin.local_test_table VALUES
            (1, 'Alice', 30),
            (2, 'Bob', 25),
            (3, 'Charlie', 35)
        """)
        print("✅ Data inserted successfully")
        
        # Query the data
        print("\n📊 Querying data:")
        spark.sql("SELECT * FROM iceberg.darwin.local_test_table").show()
        
        print("\n🎉 All tests passed! Iceberg integration is working in local mode.")
        
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        traceback.print_exc()
    finally:
        try:
            # Test AWS SDK availability
            AmazonClientException = spark._jvm.com.amazonaws.AmazonClientException
            print("✅ AWS SDK is available: ", AmazonClientException)
        except Exception as e:
            print("❌ AWS SDK class not found.")
            print("Error:", e)
        try:
            print("\nShutting down Spark session...")
            spark.stop()
        except:
            pass

if __name__ == "__main__":
    test_iceberg_local()
