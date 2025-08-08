#!/usr/bin/env python3
"""
Test script using simplified Iceberg configuration with Hadoop S3A filesystem
"""

from pyspark.sql import SparkSession

def test_iceberg_simplified():
    print("🔍 Testing Iceberg with simplified S3A configuration...")
    
    try:
        # Create Spark session with simplified Iceberg configuration
        # Remove S3FileIO and let Iceberg use default Hadoop S3A filesystem
        spark = SparkSession.builder \
            .appName("IcebergSimplifiedTest") \
            .master("spark://localhost:7077") \
            .config("spark.jars", "./jars/aws-java-sdk-bundle-1.12.262.jar,./jars/hadoop-aws-3.3.4.jar,./jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar,./jars/iceberg-aws-1.9.2.jar,./jars/aws-sdk-java-2.28.29-bundle.jar") \
            .config("spark.driver.extraClassPath", "./jars/*") \
            .config("spark.executor.extraClassPath", "/opt/spark/jars/*") \
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

        print(f"✅ Spark version: {spark.version}")
        
        # Test 1: Show catalogs (should include iceberg)
        print("\n📋 Available catalogs:")
        spark.sql("SHOW CATALOGS").show()
        
        # Test 2: Try to create an Iceberg table
        print("\n🔍 Testing Iceberg table creation...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg.darwin.test_table (
                id bigint,
                name string,
                created_at timestamp
            ) USING iceberg
        """)
        print("✅ Iceberg table created successfully!")
        
        # Test 3: Insert some test data
        print("\n🔍 Testing data insertion...")
        spark.sql("""
            INSERT INTO  iceberg.darwin.test_table VALUES 
            (1, 'test_record', current_timestamp()),
            (2, 'example_data', current_timestamp())
        """)
        print("✅ Data inserted successfully!")
        
        # Test 4: Query the data
        print("\n📊 Querying Iceberg table:")
        spark.sql("SELECT * FROM iceberg.default.test_table").show()
        
        print("\n🎉 SUCCESS! Iceberg integration with S3A is working!")
        
    except Exception as e:
        print(f"❌ Error in simplified test: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
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
    test_iceberg_simplified()
