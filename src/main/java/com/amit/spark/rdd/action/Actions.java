package com.amit.spark.rdd.action;

import com.amit.spark.Util;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;

public class Actions {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Actions").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        List<String> data = Arrays.asList(new String[]{"Amit","Idhika","Divya","Medhansh","Papa","Mummy","Papa", "Mummy"});
        List<Integer> intData = Arrays.asList(new Integer[]{1,2,3,4,5});

        JavaRDD<String> stringJavaRDD = ctx.parallelize(data);
        System.out.println("Count:"+stringJavaRDD.count());
        System.out.println("Count unique:"+stringJavaRDD.countByValue());
        System.out.println("List collected:"+stringJavaRDD.collect());
        System.out.println("Take 2:"+stringJavaRDD.take(2));

        JavaRDD<Integer> integerJavaRDD = ctx.parallelize(intData);
        integerJavaRDD.persist(StorageLevel.MEMORY_ONLY());
        Integer product = integerJavaRDD.reduce((x, y) -> x * y);
        System.out.println("Product :"+ product);
        long count = integerJavaRDD.count();
        System.out.println("Count is :" + count);

        JavaRDD<String> primeNumberRDD = ctx.textFile(Util.INPUT_PATH + "prime_nums.text");
        JavaRDD<String> lines = primeNumberRDD.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());
        Integer sum = lines.filter(number -> !number.isEmpty()).map(number -> Integer.parseInt(number)).reduce((x,y) -> x+y);
        System.out.println("Sum of prime numbers : "+ sum);

    }
}
