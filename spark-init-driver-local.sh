#!/bin/bash

curl -L -o /Users/darwinsubramaniam/Projects/big-data/jars/iceberg-spark-runtime-3.5_2.13-1.9.2.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.13/1.9.2/iceberg-spark-runtime-3.5_2.13-1.9.2.jar


# Download Iceberg Spark Runtime JAR (Scala 2.12 for Spark 3.5.6)
if [ ! -f /Users/darwinsubramaniam/Projects/big-data/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar ]; then
    curl -L -o /Users/darwinsubramaniam/Projects/big-data/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.2/iceberg-spark-runtime-3.5_2.12-1.9.2.jar
fi

# Download Iceberg AWS JAR for S3 integration
if [ ! -f /Users/darwinsubramaniam/Projects/big-data/jars/iceberg-aws-1.9.2.jar ]; then
    curl -L -o /Users/darwinsubramaniam/Projects/big-data/jars/iceberg-aws-1.9.2.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.9.2/iceberg-aws-1.9.2.jar
fi

# Download AWS SDK v1 for compatibility
if [ ! -f /Users/darwinsubramaniam/Projects/big-data/jars/aws-java-sdk-bundle-1.12.262.jar ]; then
    curl -L -o /Users/darwinsubramaniam/Projects/big-data/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
fi

# Download Hadoop AWS JAR for S3A support
if [ ! -f /Users/darwinsubramaniam/Projects/big-data/jars/hadoop-aws-3.3.4.jar ]; then
    curl -L -o /Users/darwinsubramaniam/Projects/big-data/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
fi