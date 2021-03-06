package com.amit.spark.rdd.transform;

import com.amit.spark.Util;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class WordCounter {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("WordCounter");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaRDD<String> lines = ctx.textFile(Util.INPUT_PATH + "word_count.text");
        JavaRDD<String> words  = lines.flatMap(line -> {
            return Arrays.stream(line.split(" ")).iterator();
        });
        System.out.println(words.countByValue());
    }
}
