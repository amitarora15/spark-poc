package com.amit.spark.pairrdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Creation {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("PairRDDCreator");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaRDD<String> rdd = ctx.parallelize(Arrays.asList("Amit 39", "Divya 35"));
        JavaPairRDD<String, Integer> pairRdd = rdd.mapToPair(getPair());
        pairRdd.coalesce(1).saveAsTextFile("/Users/amarora/workspace/java-poc/spark-my-poc/output/pair-rdd-creation.txt");

        List<Tuple2<String, Integer>> tuple2List = Arrays.asList(new Tuple2<>("Amit", 49), new Tuple2<>("Divya", 45));
        pairRdd = ctx.parallelizePairs(tuple2List);
        pairRdd.coalesce(1).saveAsTextFile("/Users/amarora/workspace/java-poc/spark-my-poc/output/pair-rdd-creation-from-tuple.txt");
    }

    public static PairFunction<String, String, Integer> getPair(){
        return s -> new Tuple2<>(s.split(" ")[0], Integer.parseInt(s.split(" ")[1]));
    }
}
