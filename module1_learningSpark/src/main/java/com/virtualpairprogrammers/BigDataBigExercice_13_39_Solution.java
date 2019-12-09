package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 * <p>
 * Objective:
 * Produce a ranking chart of the most popular courses, using our "Weighted Course View Ratio Process"
 * <p>
 * If a user watches more than 90% of the course, the course gets 10 points
 * If a user watches > 50% but < 90%, it scores 4
 * If a user watches > 25%, but < 50% it scores 2
 * Otherwise, no score
 * <p>
 * Which means that, as a final result, we want a dataset like:
 * <p>
 * courseId		score
 * 1			6
 * 2			10
 * 3			0
 */
public class BigDataBigExercice_13_39_Solution {

    public static void main(String[] args) {

        // require to load Hadoop libraries through winutils.exe and avoid "Unable to load native-hadoop library for your platform" issue
        System.setProperty("hadoop.home.dir", "d:/tools/winutils-extra/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Use true to use hardcoded data identical to that in the PDF guide.
        boolean testMode = false;

        // viewData: (userId, chapterId)
        JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);

        // chapterData: (chapterId, courseId)
        JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);

        // titlesData: (chapterId, title)
        JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

        // Course solution

        // WARMUP: get the count of chapters for each course (see WarmupExercice): (courseId, count of chapters)
        JavaPairRDD<Integer, Integer> courseToTalCount = chapterData
                .mapToPair(row -> new Tuple2<>(row._2, 1))
                .reduceByKey((value1, value2) -> value1 + value2);

        //        .collectAsMap();
        // We could have done it with a Map courseIdTotalCountMap, but if we need this count, it's for an upcoming
        // ENRICHMENT of the course, which can also be done through a classic (inner) join.
        // So let's keep our JavaPairRDD


        System.out.println("WARMUP: display count of chapters per course: (courseId, count of chapters)");
        // courseIdTotalCountMap.forEach((key, value) -> System.out.println("key: " + key + " value: " + value));
        courseToTalCount.foreach(v -> System.out.println(v));

        // Step 1) Removing duplicate from viewsData (userId, chapterId)
        JavaPairRDD<Integer, Integer> uniqueViewsData = viewData.distinct();
        System.out.println("Step 1: display unique views for a user: (userId, chapterId)");
        uniqueViewsData.foreach(v -> System.out.println(v));

        // Step 2a) Invert uniqueViewData to get (chapterId, userId) pairs
        JavaPairRDD<Integer, Integer> chapterUserData = uniqueViewsData.mapToPair(row -> new Tuple2<Integer, Integer>(row._2, row._1));
        System.out.println("Step 2a: invert uniqueViewData to get (chapterId, userId) pairs");
        chapterUserData.foreach(v -> System.out.println(v));

        // Step 2b) now we can (inner) join chapterUserData and chapterData
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> chapterUserCourseData = chapterUserData.join(chapterData);
        System.out.println("Step 2b: (inner) join chapterUserData and chapterData, and get (chapterId, (userId, courseId))");
        chapterUserCourseData.foreach(v -> System.out.println(v));

        // Step 3) chapter info is not useful anymore, we drop it to get (userId, courseId)
        JavaPairRDD<Integer, Integer> userCourseData = chapterUserCourseData.mapToPair(row -> new Tuple2<Integer, Integer>(row._2._1, row._2._2));
        System.out.println("Step 3: chapter info is not useful anymore, we drop it to get (userId, courseId)");
        userCourseData.foreach(v -> System.out.println(v));

        // step 4) we are going to count each different chapter viewed in a course by a user.
        // That's just a classic count of the previous dataset, meaning we need a map to get ((userId, courseId), 1), followed by
        // a reduceByKey on our new (userId, courseId) key
        JavaPairRDD<Tuple2<Integer, Integer>, Long> userCourseCountData = userCourseData
                .mapToPair(row -> new Tuple2<Tuple2<Integer, Integer>, Long>(new Tuple2<Integer, Integer>(row._1, row._2), 1L))
                .reduceByKey((value1, value2) -> value1 + value2);
        System.out.println("Step 4: count different chapter viewed in a course by a user, we get ((userId, courseId), count)");
        userCourseCountData.foreach(v -> System.out.println(v));

        // Step 5) user info is not useful anymore, we drop it to get (courseId, count)
        // To rephrase the previous step, we now read this as "someone watched 2 chapters of course 1.
        // Somebody different watched 1 chapter of course 1; someone watched 1 chapter of course 2, etc.
        JavaPairRDD<Integer, Long> courseCountData = userCourseCountData.mapToPair(row -> new Tuple2<Integer, Long>(row._1._2, row._2));
        System.out.println("Step 5: user info is not useful anymore, we drop it to get (courseId, count)");
        courseCountData.foreach(v -> System.out.println(v));

        // Step 6) to compute the percentage of the course watch by each user, we need to add the total number of chapter for each course.
        // We get (courseId, (count, totalCount))

        // As explained in the warmup exercice, rather than using a Map (which would certainly imply a round trip between driver and executor)
        // we will perform a classic (inner) join
//        JavaPairRDD<Integer, Tuple2<Long, Integer>> courseCountTotalData = courseCountData.mapToPair(row -> new Tuple2<Integer, Tuple2<Long, Integer>>(row._1, new Tuple2<>(row._2, courseIdTotalCountMap.get(row._1))));
        JavaPairRDD<Integer, Tuple2<Long, Integer>> courseCountTotalData = courseCountData.join(courseToTalCount);
        System.out.println("Step 6: add the total number of chapter for each course to get (courseId, (count, totalCount))");
        courseCountTotalData.foreach(v -> System.out.println(v));

        // Step 7) convert to percentage: (courseId, percentage)
        JavaPairRDD<Integer, Double> coursePercentData = courseCountTotalData.mapValues(value -> (double) value._1 / value._2);
        System.out.println("Step 7: convert to percentage to get (courseId, percentage)");
        coursePercentData.foreach(v -> System.out.println(v));

        // Step 8) convert to score: (courseId, score)
        JavaPairRDD<Integer, Long> courseScoreData = coursePercentData.mapValues(percentage -> {
            long score = 0;
            if (percentage > 0.9) {
                score = 10;
            } else if (percentage > 0.5 && percentage <= 0.9) {
                score = 4;
            } else if (percentage > 0.25 && percentage <= 0.5) {
                score = 2;
            }
            return score;
        });
        System.out.println("Step 8: convert to score to get (courseId, score)");
        courseScoreData.foreach(v -> System.out.println(v));

        // Step 9) sum the scores per course (courseId, totalScore)
        JavaPairRDD<Integer, Long> courseTotalScoreData = courseScoreData.reduceByKey((value1, value2) -> value1 + value2);
        System.out.println("Step 9: sum the scores per course (courseId, totalScore)");
        courseTotalScoreData.foreach(v -> System.out.println(v));

        // step 10) add the course title (meaning an ENRICHMENT) through a join (courseId, (totalScore, course title))
        JavaPairRDD<Integer, Tuple2<Long, String>> courseTotalScoreTitleData = courseTotalScoreData.join(titlesData);
        System.out.println("Step 10: add the course title (meaning an ENRICHMENT) (courseId, (totalScore, course title))");
        courseTotalScoreTitleData.foreach(v -> System.out.println(v));

        // step 11) courseId is not useful anymore, and we would like to sort the dataset by score descending order (total score, course title)
        System.out.println("Step 11: only print (course title, totalScore), ordered by score descending order");
        courseTotalScoreTitleData
                .mapToPair(row -> new Tuple2<>(row._2._1, row._2._2))
                .sortByKey(false)
                .collect()
                .forEach(v -> System.out.println(v));

        // 14.54: Caching and persistence
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        sc.close();
    }

    private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // (chapterId, title)
            List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
            rawTitles.add(new Tuple2<>(1, "How to find a better job"));
            rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
            rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
            return sc.parallelizePairs(rawTitles);
        }
        return sc.textFile("src/main/resources/viewing figures/titles.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, String>(new Integer(cols[0]), cols[1]);
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // (chapterId, (courseId, courseTitle))
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
            return sc.parallelizePairs(rawChapterData);
        }

        return sc.textFile("src/main/resources/viewing figures/chapters.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // Chapter views - (userId, chapterId)
            List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
            rawViewData.add(new Tuple2<>(14, 96));
            rawViewData.add(new Tuple2<>(14, 97));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(14, 99));
            rawViewData.add(new Tuple2<>(13, 100));
            return sc.parallelizePairs(rawViewData);
        }

        return sc.textFile("src/main/resources/viewing figures/views-*.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] columns = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
                });
    }
}
