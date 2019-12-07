package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TestingJoins_12 {

    public static void main(String[] args) {

        // require to load Hadoop libraries through winutils.exe and avoid "Unable to load native-hadoop library for your platform" issue
        System.setProperty("hadoop.home.dir","d:/tools/winutils-extra/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            // Creation of our example datasets
            List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
            usersRaw.add(new Tuple2<>(1, "John"));
            usersRaw.add(new Tuple2<>(2, "Bob"));
            usersRaw.add(new Tuple2<>(3, "Thomas"));
            usersRaw.add(new Tuple2<>(4, "Thibaut"));
            usersRaw.add(new Tuple2<>(5, "Diane"));
            usersRaw.add(new Tuple2<>(6, "Lucie"));
            System.out.println("Printing usersRaw");
            usersRaw.forEach(System.out::println);

            List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
            visitsRaw.add(new Tuple2<>(4, 18));
            visitsRaw.add(new Tuple2<>(6, 4));
            visitsRaw.add(new Tuple2<>(10, 9));
            System.out.println("Printing visitsRaw");
            visitsRaw.forEach(System.out::println);

            JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);
            System.out.println("Print users");
            users.foreach(value -> System.out.println(value));

            JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
            System.out.println("Print visits");
            visits.foreach(value -> System.out.println(value));

            JavaPairRDD<Integer, Tuple2<Integer, String>> joinRDD = visits.join(users);
            System.out.println("Print classic inner join result");
            // Careful ! Again the following foreach will be executed in a multi-threaded way
            joinRDD.foreach(value -> System.out.println(value));

            JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftJoinRDD = visits.leftOuterJoin(users);
            System.out.println("Print left outer join result");
            leftJoinRDD.foreach(value -> System.out.println(value));
            System.out.println("Print left outer join result, but just the user's name if any or blank");
            leftJoinRDD.foreach(value -> System.out.println((value._2._2.orElse("blank").toUpperCase())));

            System.out.println("Print right outer join result");
            JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightJoinRDD = visits.rightOuterJoin(users);
            rightJoinRDD.foreach(value -> System.out.println(value));
            System.out.println("Print right outer join result, but just the user's name if any or blank");
            rightJoinRDD.foreach(value -> System.out.println("User " + value._2._2 + " has " + value._2._1.orElse(0) + " visits"));

            System.out.println("Print full inner join (cartesian product) result");
            JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesianRDD = visits.cartesian(users);
            cartesianRDD.foreach(value -> System.out.println(value));

        }

    }
}
