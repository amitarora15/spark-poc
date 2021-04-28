package com.amit.spark.sql.dataset;

import com.amit.spark.Util;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;

public class DataSetOperations {

    public static void main(String[] args) {
        SparkSession spark  = SparkSession.builder().appName("Dataset operations").master("local[*]").getOrCreate();

        Dataset dataset = spark.read().option("header", "true").csv(Util.INPUT_PATH + "2016-stack-overflow-survey-responses.csv");

        dataset = dataset.select(
                col("country"),
                col("age_midpoint").as("ageMidPoint").cast("integer"),
                col("gender"));

        Dataset<StackOverFlowResponse>  stackOverFlowResponseDataset = dataset.as(Encoders.bean(StackOverFlowResponse.class));

        stackOverFlowResponseDataset.show(2);

        stackOverFlowResponseDataset.filter(stackOverFlowResponse -> "female".equalsIgnoreCase(stackOverFlowResponse.getGender())).show(2);

        stackOverFlowResponseDataset.groupBy("ageMidPoint").count().show();


        spark.stop();
    }
}
