package com.amit.spark.common;

import com.amit.spark.Util;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class BroadCast {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Broadcast").setMaster("local[2]");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        Broadcast<Map<String, String>> postalCodeMap = ctx.broadcast(loadPostCodes(ctx));
        System.out.println("Map on all worker node " + postalCodeMap.getValue());
        JavaRDD<String> dataRDD = ctx.textFile(Util.INPUT_PATH + "uk-makerspaces-identifiable-data.csv");
        JavaRDD<String> dataFilterRDD = dataRDD.filter(r -> !r.equalsIgnoreCase("Timestamp"));
        JavaRDD<String> dataFilteredMapRDD = dataFilterRDD.map(line -> {
            String words[] = line.split(Util.COMMA_DELIMITER);
            String pin = words[4];
            String postalCode = "";
            postalCode =  pin.split(" ")[0];
            if(postalCodeMap.getValue().containsKey(postalCode)){
                return postalCodeMap.getValue().get(postalCode);
            }
            return "Unknown";
        });
        System.out.println(dataFilteredMapRDD.countByValue());
    }

    public static Map<String, String> loadPostCodes(JavaSparkContext ctx){
        JavaRDD<String> postCodeStringRDD = ctx.textFile(Util.INPUT_PATH + "uk-postcode.csv");
        JavaRDD<String> postCodeFilteredRDD = postCodeStringRDD.filter(r -> !r.equalsIgnoreCase("Postcode"));
        JavaPairRDD<String, String> postCodeAreaPairRdd = postCodeFilteredRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String words[] = s.split(Util.COMMA_DELIMITER);
                return new Tuple2<>(words[0], words[7]);
            }
        });
        return postCodeAreaPairRdd.collectAsMap();
    }

}
