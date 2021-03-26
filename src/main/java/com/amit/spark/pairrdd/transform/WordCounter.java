package com.amit.spark.pairrdd.transform;

import com.amit.spark.Util;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class WordCounter {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("ParallelRDDWordCounter");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaRDD<String> lines = ctx.textFile(Util.INPUT_PATH + "word_count.text");
        JavaRDD<String> words  = lines.flatMap(line -> {
            return Arrays.stream(line.split(" ")).iterator();
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(pairMapFunction());
        JavaPairRDD<String, Integer> pairCount = pairs.reduceByKey((x, y) -> x + y);
        JavaPairRDD<Integer, String> flippedPairCount = pairCount.mapToPair(k -> new Tuple2<>(k._2, k._1));
        flippedPairCount = flippedPairCount.sortByKey(false);
        JavaPairRDD<String, Integer> flippedPairCount2 = flippedPairCount.mapToPair(pair -> new Tuple2<>(pair._2, pair._1));
        System.out.println(flippedPairCount2.collect());
    }

    private static PairFunction<String, String, Integer> pairMapFunction(){
        return line -> new Tuple2<>(line, 1);
    }

}
