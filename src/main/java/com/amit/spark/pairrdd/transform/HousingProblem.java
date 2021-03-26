package com.amit.spark.pairrdd.transform;

import com.amit.spark.Util;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

class CountPriceTuple implements Serializable {

    private int count;

    @Override
    public String toString() {
        return "CountPriceTuple{" +
                "count=" + count +
                ", price=" + price +
                '}';
    }

    private double price;

    public CountPriceTuple(int count, double price){
        this.price = price;
        this.count = count;
    }

    public Integer getCount(){
        return count;
    }

    public Double getPrice(){
        return price;
    }


}

public class HousingProblem {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("ParallelRDDHousingProblem");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaRDD<String> lines = ctx.textFile(Util.INPUT_PATH + "RealEstate.csv");
        JavaRDD<String> filteredLines = lines.filter(line -> !line.contains("MLS"));
        JavaPairRDD<Integer, CountPriceTuple> bedRoomPricePair = filteredLines.mapToPair(s -> {

                    String words[] = s.split(",");
                    Integer noOfBedRoom = Integer.valueOf(words[3]);
                    Double price = Double.valueOf(words[2]);
                    return new Tuple2<>(noOfBedRoom, new CountPriceTuple(1, price));
            });
        JavaPairRDD<Integer, CountPriceTuple> reducedBedRoomTotalPrice = bedRoomPricePair.reduceByKey((v1, v2) -> new CountPriceTuple(v1.getCount() + v2.getCount(), v1.getPrice() + v2.getPrice()));
        JavaPairRDD<Integer, CountPriceTuple> bedRoomAvgPrice = reducedBedRoomTotalPrice.mapValues(v -> new CountPriceTuple(v.getCount(), v.getPrice()/v.getCount()));
        JavaPairRDD<Integer, CountPriceTuple> sortedBedRoomAvgPrice =  bedRoomAvgPrice.sortByKey(true);
        System.out.println(sortedBedRoomAvgPrice.collect());
    }


}
