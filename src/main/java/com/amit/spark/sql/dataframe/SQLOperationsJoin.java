package com.amit.spark.sql.dataframe;

import com.amit.spark.Util;
import org.apache.spark.sql.Dataset;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession;

public class SQLOperationsJoin {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Spark-SQL-Join").master("local[*]").getOrCreate();

        Dataset refDataDf= spark.read().option("header", "true").csv(Util.INPUT_PATH+"uk-postcode.csv");
        refDataDf = refDataDf.withColumn("Postcode", concat_ws("", col("Postcode"), lit(" ")));
        refDataDf.show(3);

        Dataset tranDataDf= spark.read().option("header", "true").csv(Util.INPUT_PATH+"uk-makerspaces-identifiable-data.csv");
        Dataset joinDataDf = tranDataDf.join(refDataDf, tranDataDf.col("Postcode").startsWith(refDataDf.col("Postcode")), "left_outer");
        joinDataDf.show(3);

        joinDataDf.groupBy("Region").count().show();

        spark.stop();
    }
}
