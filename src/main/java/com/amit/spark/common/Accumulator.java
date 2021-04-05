package com.amit.spark.common;

import com.amit.spark.Util;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Option;

import java.util.Optional;

public class Accumulator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Accumulator");

        SparkContext ctx = new SparkContext(conf);
        JavaSparkContext jCtx = new JavaSparkContext(ctx);
        JavaRDD<String> responses = jCtx.textFile(Util.INPUT_PATH+"2016-stack-overflow-survey-responses.csv");

        LongAccumulator totalCountAcc = new LongAccumulator();
        totalCountAcc.register(ctx, Option.apply("Total values"), false);

        LongAccumulator missingSalAcc = new LongAccumulator();
        missingSalAcc.register(ctx, Option.apply("Missing values"), false);

        LongAccumulator bytesProcessedAcc = new LongAccumulator();
        bytesProcessedAcc.register(ctx, Option.apply("Bytes Processed"), true);

        JavaRDD<String> countryRdd = responses.filter( response -> {
            bytesProcessedAcc.add(response.getBytes().length);
            String words[] = response.split(Util.COMMA_DELIMITER, -1);

            totalCountAcc.add(1);
            if(words[14].isEmpty())
                missingSalAcc.add(1);

            return words[2].equalsIgnoreCase("Afghanistan");
        });

        System.out.println("Country data : " + countryRdd.count());
        System.out.println("Bytes processed : " + bytesProcessedAcc.value());
        System.out.println("Missing salary processed : " + missingSalAcc.value());
        System.out.println("Total processed : " + totalCountAcc.value());
        System.out.println("Total processed Avg : " + totalCountAcc.avg());
        System.out.println("Total processed Count : " + totalCountAcc.count());
        System.out.println("Total processed Failed Values : " + totalCountAcc.countFailedValues());
        System.out.println("Total processed At driver side? : " + totalCountAcc.isAtDriverSide());
        System.out.println("Total processed meta : " + totalCountAcc.metadata());
    }
}
