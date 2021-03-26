package com.amit.spark.pairrdd.transform;

import com.amit.spark.Util;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class FilterAndMap {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("PairRDDTransformations");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        JavaRDD<String> rdd = ctx.textFile(Util.INPUT_PATH + "airports.text");
        JavaPairRDD<String, String> pairRDD = rdd.mapToPair(new PairFunction<String, String, String>() {

            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split(Util.COMMA_DELIMITER)[1], s.split(Util.COMMA_DELIMITER)[3]);
            }
        });

        pairRDD = pairRDD.filter(x -> !x._2.equalsIgnoreCase("\"United States\""));
        pairRDD = pairRDD.mapValues(k -> k.toUpperCase());
        pairRDD.saveAsTextFile(Util.OUTPUT_PATH + "upper-filtered-pair-rdd.txt");
    }

}
