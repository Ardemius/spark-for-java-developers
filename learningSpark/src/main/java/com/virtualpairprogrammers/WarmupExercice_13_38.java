package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class WarmupExercice_13_38 {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        WarmupExercice_13_38 exercice = new WarmupExercice_13_38();
        List<Tuple2<Integer, Integer>> datasetRaw = exercice.initSampleDataset();

        // init Spark framework
        SparkConf conf = new SparkConf().setAppName("WarmupExercice").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // we want to count the number of chapter of EACH course
        JavaPairRDD<Integer, Integer> dataset = sc.parallelizePairs(datasetRaw);
        // the usual count solution: map each occurrence of a chapter in a course to the "1" value
        System.out.println("Print count of chapter occurrences per course");
        dataset
                .mapToPair(course -> new Tuple2<>(course._2, 1))
                .reduceByKey((value1, value2) -> value1 + value2)
                .sortByKey()
                .take(10)
                .forEach(value -> System.out.println(value));

    }

    private List<Tuple2<Integer, Integer>> initSampleDataset() {

        // Tuple2 structure: chapterId, courseId
        List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
        rawChapterData.add(new Tuple2<>(96, 1));
        rawChapterData.add(new Tuple2<>(97, 1));
        rawChapterData.add(new Tuple2<>(98, 1));
        rawChapterData.add(new Tuple2<>(99, 2));
        rawChapterData.add(new Tuple2<>(100, 3));
        rawChapterData.add(new Tuple2<>(101, 3));
        rawChapterData.add(new Tuple2<>(102, 3));
        rawChapterData.add(new Tuple2<>(103, 3));
        rawChapterData.add(new Tuple2<>(104, 3));
        rawChapterData.add(new Tuple2<>(105, 3));
        rawChapterData.add(new Tuple2<>(106, 3));
        rawChapterData.add(new Tuple2<>(107, 3));
        rawChapterData.add(new Tuple2<>(108, 3));
        rawChapterData.add(new Tuple2<>(109, 3));

        return rawChapterData;
    }
}
