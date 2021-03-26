package com.amit.spark.pairrdd.transform;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.Arrays;

public class JoinOperations {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Join").setMaster("local[2]");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> nameAgePairRDD = ctx.parallelizePairs(Arrays.asList(new Tuple2<String, Integer>("Amit",39), new Tuple2<String, Integer>("Divya",35), new Tuple2<String, Integer>("Medhansh",7)));
        JavaPairRDD<String, String> nameProfessionPairRDD = ctx.parallelizePairs(Arrays.asList(new Tuple2<String, String>("Amit","IT"), new Tuple2<String, String>("Divya","Dentist"), new Tuple2<String, String>("Idhika","Student")));

        nameAgePairRDD.partitionBy(new HashPartitioner(4));
        nameProfessionPairRDD.partitionBy(new HashPartitioner(4));

        JavaPairRDD<String, Tuple2<Integer, String>> nameAgeAddressCommonRDD = nameAgePairRDD.join(nameProfessionPairRDD);
        System.out.println(nameAgeAddressCommonRDD.collect());

        JavaPairRDD<String, Tuple2<Integer, Optional<String>>> nameAgeAddressLeftRDD = nameAgePairRDD.leftOuterJoin(nameProfessionPairRDD);
        System.out.println(nameAgeAddressLeftRDD.collectAsMap());

        JavaPairRDD<String, Tuple2<Optional<Integer>, String>> nameAgeAddressRightRDD = nameAgePairRDD.rightOuterJoin(nameProfessionPairRDD);
        System.out.println(nameAgeAddressLeftRDD.collectAsMap());

        JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<String>>> nameAgeAddressFullJoinRDD = nameAgePairRDD.fullOuterJoin(nameProfessionPairRDD);
        System.out.println(nameAgeAddressFullJoinRDD.collectAsMap());
    }
}
