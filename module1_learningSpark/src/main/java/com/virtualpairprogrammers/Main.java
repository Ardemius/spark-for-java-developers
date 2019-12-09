package com.virtualpairprogrammers;

import com.google.common.collect.Iterables;
import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {

        // just to avoid too many unnecessary logs
        // Be careful to use a org.apache.log4j.Logger and no other!
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // require to load Hadoop libraries through winutils.exe and avoid "Unable to load native-hadoop library for your platform" issue
        System.setProperty("hadoop.home.dir", "d:/tools/winutils-extra/hadoop");

        // upToLecture11();
        // lecture13Tuples();
        // lecture15PairRDD();
        // lecture16ReduceByKey();
        // lecture17FluentAPI();
        // lecture18GroupByKey();
//        lecture19FlatMaps();
        // lecture20Filters();
        // lecture21ReadingFromDisk();
        //lecture22KeywordRankingPracticalMyTry();
//        lecture22KeywordRankingPracticalWorkedSolution();
//        lecture49AccessDAGthroughLocalhost4040();
//        lecture51Shuffles();
//        lecture52DealingWithKeySkewsAndSalting();
        lecture54CachingAndPersistence();
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

    private static void lecture13Tuples() {

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

    private static List<String> getLogSample() {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");
        return inputData;
    }

    private static void lecture15PairRDD() {

        List<String> inputData = getLogSample();

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            JavaRDD<String> originalLogMessages = sc.parallelize(inputData);
            JavaPairRDD<String, String> pairRDD = originalLogMessages.mapToPair(rawValue -> {
                String[] columns = rawValue.split(":");
                String level = columns[0];
                String date = columns[1];

                return new Tuple2<>(level, date);
            });

            pairRDD.foreach(tuple -> System.out.println(tuple._1 + " happened the " + tuple._2));
        }
    }

    private static void lecture16ReduceByKey() {

        List<String> inputData = getLogSample();

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            System.out.println("Counting types of log level with");

            JavaRDD<String> originalLogMessages = sc.parallelize(inputData);
            JavaPairRDD<String, Long> pairRDD = originalLogMessages.mapToPair(rawValue -> {
                String[] columns = rawValue.split(":");
                String level = columns[0];

                return new Tuple2<>(level, 1L);
            });

            JavaPairRDD<String, Long> sumsRDD = pairRDD.reduceByKey((value1, value2) -> value1 + value2);
            sumsRDD.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
        }
    }

    private static void lecture17FluentAPI() {

        System.out.println("---- lecture17FluentAPI");

        List<String> inputData = getLogSample();

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            System.out.println("Counting types of log level with a FLUENT API");

            sc.parallelize(inputData)
                    .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                    .reduceByKey((value1, value2) -> value1 + value2)
                    .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
        }
    }

    private static void lecture18GroupByKey() {

        System.out.println("---- lecture18GroupByKey");

        List<String> inputData = getLogSample();

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            System.out.println("Use of a groupByKey, which is NOT AT ALL RECOMMENDED for performances reasons");

            sc.parallelize(inputData)
                    .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                    .groupByKey()
                    .foreach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances"));
        }
    }

    private static void lecture19FlatMaps() {

        System.out.println("---- lecture19FlatMaps");

        List<String> inputData = getLogSample();

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            JavaRDD<String> sentences = sc.parallelize(inputData);
            JavaRDD<String> words = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());

            System.out.println("Print sentences");
            sentences.collect().forEach(System.out::println);
            System.out.println("Print words");
            words.collect().forEach(System.out::println);
        }
    }

    private static void lecture20Filters() {

        System.out.println("---- lecture20Filters");

        List<String> inputData = getLogSample();

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            System.out.println("Only print word which length > 1");

            sc.parallelize(inputData)
                    .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                    .filter(word -> word.length() > 1)
                    .collect()
                    .forEach(System.out::println);

        }
    }

    private static void lecture21ReadingFromDisk() {

        System.out.println("---- lecture21ReadingFromDisk");

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");
            initialRDD
                    .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                    .collect()
                    .forEach(System.out::println);
        }
    }

    private static void lecture22KeywordRankingPracticalMyTry() {

        System.out.println("---- lecture22KeywordRankingPracticalMyTry");

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            JavaRDD<String> boringWords = sc.textFile("src/main/resources/subtitles/boringwords.txt");

            JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");
            initialRDD
                    // 1) get words from input.txt
                    .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
                    // 2a) get rid of some unwanted characters (keeping only letter), and have all words lowercase
                    .map(anyWord -> anyWord.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
                    // getting rid of count of blanks coming from previous replacement
                    .filter(letterWord -> letterWord.length() > 0)
                    // 2b) filter words that are boring (only keep words that are not boring)
                    .filter(word -> Util.isNotBoring(word))
                    // 3) count occurences of every remaining words (using the method of lecture 16 and 17)
                    .mapToPair(word -> new Tuple2<>(word, 1L))
                    .reduceByKey((value1, value2) -> value1 + value2)
                    // 4) now we need to sort the words by their occurrence
                    // to do so, let's transform <blabla, 3> in <3, blabla> to be able to sort those new tuple by key
                    .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                    .sortByKey(false)
                    // 5) finally we take the 10 bigger
                    .take(10)
                    .forEach(System.out::println);
        }
    }

    private static void lecture22KeywordRankingPracticalWorkedSolution() {

        System.out.println("---- lecture22KeywordRankingPracticalWorkedSolution");

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

            JavaRDD<String> lettersOnlyRdd = initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());

            JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter(sentence -> sentence.trim().length() > 0);

            JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());

            JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);

            JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(word -> Util.isNotBoring(word));

            JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L));

            JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((value1, value2) -> value1 + value2);

            JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1));

            JavaPairRDD<Long, String> sorted = switched.sortByKey(false);

            int numPartitions = sorted.getNumPartitions();
            System.out.println("Number of partitions at this level: " + numPartitions);
            // This foreach will NOT give a correct sort, as its associated lambda is going to be executed in parallel by multiple threads
            //sorted.foreach(element -> System.out.println(element));

            List<Tuple2<Long, String>> results = sorted.take(10);

            results.forEach(System.out::println);
        }
    }

    private static void lecture49AccessDAGthroughLocalhost4040() {

        System.out.println("---- lecture49AccessDAGthroughLocalhost4040");

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

            JavaRDD<String> lettersOnlyRdd = initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());

            JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter(sentence -> sentence.trim().length() > 0);

            JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());

            JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);

            JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(word -> Util.isNotBoring(word));

            JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L));

            JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((value1, value2) -> value1 + value2);

            JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1));

            JavaPairRDD<Long, String> sorted = switched.sortByKey(false);

            int numPartitions = sorted.getNumPartitions();
            System.out.println("Number of partitions at this level: " + numPartitions);
            // This foreach will NOT give a correct sort, as its associated lambda is going to be executed in parallel by multiple threads
            //sorted.foreach(element -> System.out.println(element));

            List<Tuple2<Long, String>> results = sorted.take(10);

            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();

            results.forEach(System.out::println);
        }
    }

    private static void lecture51Shuffles() {

        System.out.println("---- lecture51Shuffles");

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            // Q: How will this initialRdd be partitioned ?
            // A: 64 Mo blocks (2018, could be 128 201912). So there will be about 6 partitions
            JavaRDD<String> initialRdd = sc.textFile("src/main/resources/bigLog.txt");
            // key: log level
            // value: date
            System.out.println("Initial RDD Partition size: " + initialRdd.getNumPartitions());

            JavaPairRDD<String, String> warningsAgainstDate = initialRdd.mapToPair(inputLine -> {
                String[] cols = inputLine.split(":");
                String level = cols[0];
                String date = cols[1];
                return new Tuple2<>(level, date);
            });

            System.out.println("After a narrow transformation, we have " + warningsAgainstDate.getNumPartitions() + " parts");

            // Now we're going to do a "wide" transformation
            JavaPairRDD<String, Iterable<String>> results = warningsAgainstDate.groupByKey();

            System.out.println(results.getNumPartitions() + " partitions after the wide transformation");

            results.foreach(it -> System.out.println("key " + it._1 + " has " + Iterables.size(it._2) + " elements"));

            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
        }
    }

    private static void lecture52DealingWithKeySkewsAndSalting() {

        System.out.println("---- lecture52DealingWithKeySkewsAndSalting");

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            // Q: How will this initialRdd be partitioned ?
            // A: 64 Mo blocks (2018, could be 128 201912). So there will be about 6 partitions
            JavaRDD<String> initialRdd = sc.textFile("src/main/resources/bigLog.txt");
            // key: log level
            // value: date
            System.out.println("Initial RDD Partition size: " + initialRdd.getNumPartitions());

            JavaPairRDD<String, String> warningsAgainstDate = initialRdd.mapToPair(inputLine -> {
                String[] cols = inputLine.split(":");
                // We are just adding SALT here, to "spread" our keys among all the partitions
                String level = cols[0] + (int) (Math.random() * 11);
                String date = cols[1];
                return new Tuple2<>(level, date);
            });

            System.out.println("After a narrow transformation, we have " + warningsAgainstDate.getNumPartitions() + " parts");

            // Now we're going to do a "wide" transformation
            JavaPairRDD<String, Iterable<String>> results = warningsAgainstDate.groupByKey();

            System.out.println(results.getNumPartitions() + " partitions after the wide transformation");

            results.foreach(it -> System.out.println("key " + it._1 + " has " + Iterables.size(it._2) + " elements"));

            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
        }
    }

    private static void lecture54CachingAndPersistence() {

        System.out.println("---- lecture54CachingAndPersistence");

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            // Q: How will this initialRdd be partitioned ?
            // A: 64 Mo blocks (2018, could be 128 201912). So there will be about 6 partitions
            JavaRDD<String> initialRdd = sc.textFile("src/main/resources/bigLog.txt");
            // key: log level
            // value: date
            System.out.println("Initial RDD Partition size: " + initialRdd.getNumPartitions());

            JavaPairRDD<String, String> warningsAgainstDate = initialRdd.mapToPair(inputLine -> {
                String[] cols = inputLine.split(":");
                // We are just adding SALT here, to "spread" our keys among all the partitions
                String level = cols[0] + (int) (Math.random() * 11);
                String date = cols[1];
                return new Tuple2<>(level, date);
            });

            System.out.println("After a narrow transformation, we have " + warningsAgainstDate.getNumPartitions() + " parts");

            // Now we're going to do a "wide" transformation
            JavaPairRDD<String, Iterable<String>> results = warningsAgainstDate.groupByKey();

            // First solution to avoid the redoing of all transformations described below: write data to cache
            // It creates a king of "checkpoint", storing previous "results" physically in memory
            // The data cached will then be used for the upcoming foreach and count
            // results = results.cache();

            // Alternative to "cache()", persist()
            // When used with StorageLevel.MEMORY, same thing as "cache()"
            // With MEMORY_AND_DISK, it will first try to store data in memory, and if it can't, it will write data to disk
            results = results.persist(StorageLevel.MEMORY_AND_DISK());

            System.out.println(results.getNumPartitions() + " partitions after the wide transformation");

            results.foreach(it -> System.out.println("key " + it._1 + " has " + Iterables.size(it._2) + " elements"));

            // the terrible count that requires to redo allllll transformations from initialRdd sc.textFile...
            // That's one of the secret of Spark, no intermediary result are kept in memory: it requires constant recalculations,
            // for every ACTIONS
            System.out.println(results.count());

            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
        }

    }

}
