package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class Main {

    public static void main(String[] args) {
        // just to avoid too many unnecessary logs
        // Be careful to use a org.apache.log4j.Logger and no other!
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // require to load Hadoop libraries through winutils.exe and avoid "Unable to load native-hadoop library for your platform" issue
        System.setProperty("hadoop.home.dir", "d:/tools/winutils-extra/hadoop");

        //lecture58DatasetBasics();
        //lecture59FiltersWithExpression();
        //lecture60FiltersUsingLambdas();
        lecture61FiltersUsingColumns();
    }

    private static void lecture58DatasetBasics() {

        System.out.println("---- lecture58DatasetBasics");

        try (SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                // the next line is only for Windows
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
            dataset.show();

            long numberOfRows = dataset.count();
            System.out.println("There are " + numberOfRows + " records");

            Row firstRow = dataset.first();

            String subject = firstRow.get(2).toString();
            System.out.println("1st row subject is: " + subject);
            subject = firstRow.getAs("subject").toString();
            System.out.println("again: " + subject);

            int year = Integer.parseInt(firstRow.getAs("year"));
            System.out.println("1st row year is: " + year);
        }
    }

    private static void lecture59FiltersWithExpression() {

        System.out.println("---- lecture59FiltersWithExpression");

        try (SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                // the next line is only for Windows
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

            Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007");

            modernArtResults.show();

        }
    }

    /**
     * This way of doing is NOT the more convenient / user friendly one (with SQL expressions is better)
     */
    private static void lecture60FiltersUsingLambdas() {

        System.out.println("---- lecture60FiltersUsingLambdas");

        try (SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                // the next line is only for Windows
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

            // Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007");
            Dataset<Row> modernArtResults = dataset.filter((FilterFunction<Row>) row -> row.getAs("subject").equals("Modern Art")
                    && Integer.parseInt(row.getAs("year")) >= 2007);

            modernArtResults.show();

        }
    }

    private static void lecture61FiltersUsingColumns() {

        System.out.println("---- lecture61FiltersUsingColumns");

        try (SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                // the next line is only for Windows
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

            Column subjectColumn = dataset.col("subject");
            Column yearColumn = dataset.col("year");

            Dataset<Row> modernArtResults = dataset.filter(subjectColumn.equalTo("Modern Art")
                    .and(yearColumn.geq(2007)));

            modernArtResults.show();

            // now using the "functions" class
            //Column newSubjectColumn = col("subject");
            Dataset<Row> otherModernArtResults = dataset.filter(col("subject").equalTo("Modern Art")
                    .and(col("year").geq(2007)));

            otherModernArtResults.show();
        }
    }

}
