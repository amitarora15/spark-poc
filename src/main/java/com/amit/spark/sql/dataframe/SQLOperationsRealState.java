package com.amit.spark.sql.dataframe;

import com.amit.spark.Util;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class SQLOperationsRealState {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Spark-SQL-Real-State").master("local[*]").getOrCreate();
        Dataset<Row> dataset = spark.read().option("header", true).csv(Util.INPUT_PATH+"RealEstate.csv");

        dataset = dataset.withColumn("Price SQ Ft", col("Price SQ Ft").cast("float")).withColumn("Price", col("Price").cast("float"));
        dataset.printSchema();
        dataset.groupBy("Location").agg(avg("Price SQ Ft").as("Avg Price SQ Ft"), max("Price").alias("Max Price")).orderBy(desc("Avg Price SQ Ft")).show(5);
        spark.stop();
    }
}
