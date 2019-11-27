package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        // just to avoid too many non necessary log
        // Be careful to use a org.apache.log4j.Logger and no other!
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        List<Double> inputData = new ArrayList<>();
        inputData.add(10.1);
        inputData.add(11.2);
        inputData.add(12.3);
        inputData.add(50.6);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<Double> myRDD = sc.parallelize(inputData);
            Double result = myRDD.reduce((value1, value2) -> value1 + value2);

            System.out.println(result);
        }



    }
}
