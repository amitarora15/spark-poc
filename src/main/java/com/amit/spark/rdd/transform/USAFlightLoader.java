package com.amit.spark.rdd.transform;

import com.amit.spark.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.codehaus.janino.Java;

public class USAFlightLoader {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("USAFlight");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaRDD<String> flights = ctx.textFile(Util.INPUT_PATH + "airports.text");
        JavaRDD<String> filteredFlights = flights.filter(x -> x.split(Util.COMMA_DELIMITER)[3].equalsIgnoreCase("\"United States\""));
        JavaRDD<String> output = filteredFlights.map(flight -> {
            String[] flightArray = flight.split(Util.COMMA_DELIMITER);
            return StringUtils.join(new String[]{flightArray[1], flightArray[2]}, ",");
        });
        output.saveAsTextFile(Util.OUTPUT_PATH + "us_airports.txt");

        JavaRDD<String> latFilter = filteredFlights.filter(x -> {
            String lattitude = x.split(Util.COMMA_DELIMITER)[6];
            return Float.parseFloat(lattitude) > 50;
        });
        JavaRDD<String> outputWithLatFilter = latFilter.map(flight -> {
            String[] flightArray = flight.split(Util.COMMA_DELIMITER);
            return StringUtils.join(new String[]{flightArray[1], flightArray[2], flightArray[6]}, ",");
        });
        outputWithLatFilter.saveAsTextFile(Util.OUTPUT_PATH + "us_airports_latGT50.txt");
        //System.out.println(filteredFlights.count());
    }
}
