package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 *
 * Objective:
 * Produce a ranking chart of the most popular courses, using our "Weighted Course View Ratio Process"
 *
 * If a user watches more than 90% of the course, the course gets 10 points
 * If a user watches > 50% but < 90%, it scores 4
 * If a user watches > 25%, but < 50% it scores 2
 * Otherwise, no score
 *
 * Which means that, as a final result, we want a dataset like:
 *
 * courseId		score
 * 		1			6
 * 		2			10
 * 		3			0
 */
public class BigDataBigExercice_13_39_Work
{
	public static void main(String[] args)
	{
		// require to load Hadoop libraries through winutils.exe and avoid "Unable to load native-hadoop library for your platform" issue
		System.setProperty("hadoop.home.dir","d:/tools/winutils-extra/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = true;

		// viewData: (userId, chapterId)
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);

		// chapterData: (chapterId, courseId)
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);

		// titlesData: (chapterId, title)
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

		// MY SOLUTION

		// 1) get the count of chapters for each course (see WarmupExercice)
		List<Tuple2<Integer, Integer>> countChapterPerCourseList = chapterData
				.mapToPair(row -> new Tuple2<>(row._2, 1))
				.reduceByKey((value1, value2) -> value1 + value2)
				.collect();

		System.out.println("Display count of chapters per course");
		countChapterPerCourseList.forEach(value -> System.out.println(value));

		// 2) Invert viewData to get chapter / user pairs list
		JavaPairRDD<Integer, Integer> chapterUserData = viewData.mapToPair(row -> new Tuple2<>(row._2, row._1));
		System.out.println("Print chapter / user pairs list");
		chapterUserData.foreach(v -> System.out.println(v));

		// 3) now we can (inner) join chapterUserData and chapterData
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> chapterUserCourse = chapterUserData.join(chapterData);
		System.out.println("Print chapter / (user / course) list");
		chapterUserCourse.foreach(v -> System.out.println(v));

		// we need to make further processes at course level, meaning a new mapping to get course / user / chapter
		//JavaRDD<Tuple2> courseUserChapter = chapterUserCourse.map(row -> new Tuple2(row._2._2, new Tuple2<>(row._2._1, row._1)));
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> courseUserChapter = chapterUserCourse.mapToPair(row -> new Tuple2(row._2._2, new Tuple2<Integer, Integer>(row._2._1, row._1)));
		//JavaPairRDD<Integer, Tuple2<Integer, Integer>> chapterUserCoursePairRDD = (JavaPairRDD<Integer, Tuple2<Integer, Integer>>) JavaPairRDD.fromJavaRDD(chapterUserCourse);
		System.out.println("Print course / (user / chapter) list");
		courseUserChapter.foreach(v -> System.out.println(v));

		// 4) get course / (user, count of chapter)
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> courseUserOne = courseUserChapter.mapToPair(row -> new Tuple2(row._1, new Tuple2(row._2._1, 1)));
		System.out.println("Print course / (user / ONE) list");
		courseUserOne.foreach(v -> System.out.println(v));

		// 5) et là je suis coincé... J'ai besoin d'un réduction sur le user (le 2e valeur de mon triplet), et je ne sais pas comment faire...
		// -> et en fait, jusqu'ici, c'était tout bon !
		// pour mon problème de réduction, il faut juste transformer la clé elle-même en un tuple,  (course, user) en l'occurence
		// On va continuer dans la classe BigDataBigExercice_13_39_Solution

		sc.close();
	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
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
														return new Tuple2<Integer, String>(new Integer(cols[0]),cols[1]);
				                                    });
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));
			rawChapterData.add(new Tuple2<>(99,  2));
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
		
		if (testMode)
		{
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}
		
		return sc.textFile("src/main/resources/viewing figures/views-*.csv")
				     .mapToPair(commaSeparatedLine -> {
				    	 String[] columns = commaSeparatedLine.split(",");
				    	 return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
				     });
	}
}
