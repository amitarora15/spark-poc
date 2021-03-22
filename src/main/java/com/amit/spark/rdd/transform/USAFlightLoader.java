package com.amit.spark.rdd.transform;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.codehaus.janino.Java;

public class USAFlightLoader {

    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("USAFlight");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaRDD<String> flights = ctx.textFile("/Users/amarora/workspace/java-poc/spark-my-poc/input/airports.text");
        JavaRDD<String> filteredFlights = flights.filter(x -> x.split(COMMA_DELIMITER)[3].equalsIgnoreCase("\"United States\""));
        JavaRDD<String> output = filteredFlights.map(flight -> {
            String[] flightArray = flight.split(COMMA_DELIMITER);
            return StringUtils.join(new String[]{flightArray[1], flightArray[2]}, ",");
        });
        output.saveAsTextFile("/Users/amarora/workspace/java-poc/spark-my-poc/output/us_airports.txt");

        JavaRDD<String> latFilter = filteredFlights.filter(x -> {
            String lattitude = x.split(COMMA_DELIMITER)[6];
            return Float.parseFloat(lattitude) > 50;
        });
        JavaRDD<String> outputWithLatFilter = latFilter.map(flight -> {
            String[] flightArray = flight.split(COMMA_DELIMITER);
            return StringUtils.join(new String[]{flightArray[1], flightArray[2], flightArray[6]}, ",");
        });
        outputWithLatFilter.saveAsTextFile("/Users/amarora/workspace/java-poc/spark-my-poc/output/us_airports_latGT50.txt");
        //System.out.println(filteredFlights.count());
    }
}
