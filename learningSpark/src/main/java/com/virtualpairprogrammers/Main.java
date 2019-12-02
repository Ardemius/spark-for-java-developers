package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        // just to avoid too many unnecessary logs
        // Be careful to use a org.apache.log4j.Logger and no other!
        Logger.getLogger("org.apache").setLevel(Level.WARN);

//        upToLecture11();
        upToLectureNext();

    }

    private static void upToLecture11() {

        List<Integer> inputData = new ArrayList<>();
        inputData.add(10);
        inputData.add(11);
        inputData.add(12);
        inputData.add(50);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            // section 2: Getting Started
            // 6. Installing Spark
            JavaRDD<Integer> myRDD = sc.parallelize(inputData);

            // section 3: Reduces on RDDs
            // 7: Reduces on RDD
            Integer result = myRDD.reduce((value1, value2) -> value1 + value2);
            System.out.println("Sum of the values: " + result);

            // section 4: Mapping and Outputting
            // 8: Mapping operations
            JavaRDD<Double> sqrtRDD = myRDD.map(value -> Math.sqrt(value));

            // 9: Outputting results to the console
            System.out.println("Printing sqrtRDD values through a FOREACH");
            System.out.println("a) with a classic lambda");
            sqrtRDD.foreach(value -> System.out.println(value));

            // other possibility, using a Java 8 reference method
            //sqrtRDD.foreach( System.out::println );
            // 11: NotSerializableException
            // Exception in thread "main" org.apache.spark.SparkException: Task not serializable
            // the previous row will NOT work on my computer as it uses multiple physical CPUs (12 indeed),
            // thus requiring System.out::println to be Serializable, which it is NOT.
            // If I only had a computer with a single CPU, this problem would not happen.
            // To avoid it even in my case, we need to transform our RDD to something else, like a classic Java List
            // which can be done with a JavaRDDLike.collect method. Once we have a List, everything is "really" local,
            // and the serialization matter does not exist anymore
            System.out.println("b) with a method reference AND a PRIOR JavaRDDLike.collect");
            sqrtRDD.collect().forEach(System.out::println);

            // 10: counting Big Data items
            // with a classic JavaRDDLike.count method
            System.out.println("Counting the number of elements of sqrtRDD: " + sqrtRDD.count());
            // and the same result but this time using a mix of map and reduce
            JavaRDD<Long> mapToOneRDD = sqrtRDD.map(value -> Long.valueOf(1)); // just a simple "1L" would have been enough
            Long count = mapToOneRDD.reduce((value1, value2) -> value1 + value2);
            System.out.println("Same count given by a mix of map and reduce: " + count);
        }
    }

    private static void upToLectureNext() {

        List<Integer> inputData = new ArrayList<>();
        inputData.add(10);
        inputData.add(11);
        inputData.add(12);
        inputData.add(50);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);
            JavaRDD<Tuple2<Integer, Double>> sqrtRDD = originalIntegers.map(value -> new Tuple2<Integer, Double>(value, Math.sqrt(value)));
            System.out.println("Printing RDD made of Tuple2 (value and its square root)");
            sqrtRDD.foreach(value -> System.out.println(value));


        }

    }
}
