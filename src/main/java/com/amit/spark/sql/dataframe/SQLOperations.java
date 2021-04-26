package com.amit.spark.sql.dataframe;

import com.amit.spark.Util;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession;

public class SQLOperations {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Spark-SQL");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv(Util.INPUT_PATH+"2016-stack-overflow-survey-responses.csv");

        dataset=dataset.withColumn("salary_midpoint", col("salary_midpoint").cast("integer")).withColumn("age_midpoint", col("age_midpoint").cast("integer"));
        dataset.printSchema();

        dataset.show(2);
        dataset.select(col("so_region")).distinct().show(2);

        dataset.filter(col("so_region").equalTo("South Asia")).select(col("experience_range"), col("age_range")).
                orderBy(col("age_range")).show(2);

        dataset.groupBy(col("so_region")).count().show(5);

        dataset.groupBy(col("so_region")).agg(avg("salary_midpoint").as("avg_salary"), max("age_midpoint").alias("max_age")).show(5);
        spark.stop();
    }
}
