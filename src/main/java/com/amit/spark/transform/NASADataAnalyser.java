package com.amit.spark.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class NASADataAnalyser {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("NASAFlight");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaRDD<String> nasaJulyData = ctx.textFile("/Users/amarora/workspace/java-poc/spark-my-poc/input/nasa_19950701.tsv");
        JavaRDD<String> filteredNasaJulyData = nasaJulyData.filter(line -> !line.contains("host"));
        JavaRDD<String> nasaAugData = ctx.textFile("/Users/amarora/workspace/java-poc/spark-my-poc/input/nasa_19950801.tsv");
        JavaRDD<String> filteredNasaAugData = nasaAugData.filter(line -> !line.contains("host"));
        filteredNasaJulyData.union(filteredNasaAugData).sample(true, 0.2).saveAsTextFile("/Users/amarora/workspace/java-poc/spark-my-poc/output/merged_nasa_data.tsv");

       JavaRDD<String> julyHosts = filteredNasaJulyData.map(line -> {
            String[] lines = line.split("\t");
            return lines[0];
        });
        JavaRDD<String> augHosts = filteredNasaAugData.map(line -> {
            String[] lines = line.split("\t");
            return lines[0];
        });
        JavaRDD<String> commonHosts = julyHosts.intersection(augHosts);
        commonHosts.saveAsTextFile("/Users/amarora/workspace/java-poc/spark-my-poc/output/nasa-common-host.tsv");
    }
}
