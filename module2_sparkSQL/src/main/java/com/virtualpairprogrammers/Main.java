package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

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
        //lecture61FiltersUsingColumns();
        //testDisplayConf();
        //lecture62FullSQLSyntax();
        //lecture63InMemoryDataAndSchema();
        //lecture64GroupingAndAggregations();
        //lecture65DateFormatting();
        //lecture66MultipleGrouping();
        //lecture66MultipleGroupingBigLog();
//        lecture67Ordering();
//        lecture68DataFrameAPI();
        lecture69DataFrameGrouping();
    }

    private static void lecture58DatasetBasics() {

        System.out.println("---- lecture58DatasetBasics");

        try (SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
                // the next line is only for Windows
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");
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

        try (SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
                // the next line is only for Windows
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");

            Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007");

            modernArtResults.show();

        }
    }

    /**
     * This way of doing is NOT the more convenient / user friendly one (with SQL expressions is better)
     */
    private static void lecture60FiltersUsingLambdas() {

        System.out.println("---- lecture60FiltersUsingLambdas");

        try (SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
                // the next line is only for Windows
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");

            // Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007");
            Dataset<Row> modernArtResults = dataset.filter((FilterFunction<Row>) row -> row.getAs("subject").equals("Modern Art")
                    && Integer.parseInt(row.getAs("year")) >= 2007);

            modernArtResults.show();

        }
    }

    private static void lecture61FiltersUsingColumns() {

        System.out.println("---- lecture61FiltersUsingColumns");

        try (SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
                // the next line is only for Windows
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");

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

    private static void testDisplayConf() {

        System.out.println("---- testDisplayConf");

        try (SparkSession sparkSession = SparkSession.builder().appName("testDisplayConf").master("local[*]")
                // the next line is only for Windows
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            Map<String, String> scalaMap = sparkSession.conf().getAll();
            //JavaConverters.asJavaIterableConverter(scalaMap).asJava().forEach(v -> System.out.println(v));
            java.util.Map<String, String> javaMap = JavaConverters.mapAsJavaMapConverter(scalaMap).asJava();
            javaMap.forEach((v1, v2) -> System.out.println("entry " + v1 + " : " + v2));
        }

    }

    private static void lecture62FullSQLSyntax() {

        System.out.println("---- lecture62FullSQLSyntax");

        try (SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
                // the next line is only for Windows
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            Dataset<Row> dataframe = sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");
            dataframe.createOrReplaceTempView("my_student_view");

            Dataset<Row> results = sparkSession.sql("select distinct(year) from my_student_view order by year desc");

            results.show();
        }
    }

    private static void lecture63InMemoryDataAndSchema() {

        System.out.println("---- lecture63InMemoryDataAndSchema");

        try (SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            List<Row> inMemoryData = new ArrayList<>();
            inMemoryData.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
            inMemoryData.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
            inMemoryData.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
            inMemoryData.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
            inMemoryData.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));

            // creation of the schema
            StructField[] schemaFields = new StructField[]{
                    new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
            };
            StructType schema = new StructType(schemaFields);

            Dataset<Row> dataFrame = sparkSession.createDataFrame(inMemoryData, schema);

            dataFrame.show();
        }
    }

    private static void lecture64GroupingAndAggregations() {

        System.out.println("---- lecture64GroupingAndAggregations");

        try (SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            List<Row> inMemoryData = new ArrayList<>();
            inMemoryData.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
            inMemoryData.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
            inMemoryData.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
            inMemoryData.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
            inMemoryData.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));

            // creation of the schema
            StructField[] schemaFields = new StructField[]{
                    new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
            };
            StructType schema = new StructType(schemaFields);

            Dataset<Row> dataFrame = sparkSession.createDataFrame(inMemoryData, schema);
            dataFrame.createOrReplaceTempView("logging_table");

            Dataset<Row> resultsCount = sparkSession.sql("select level, count(datetime) from logging_table group by level order by level");
            resultsCount.show();
            Dataset<Row> resultsCollectList = sparkSession.sql("select level, collect_list(datetime) from logging_table group by level order by level");
            resultsCollectList.show();
        }
    }

    private static void lecture65DateFormatting() {

        System.out.println("---- lecture65DateFormatting");

        try (SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            List<Row> inMemoryData = new ArrayList<>();
            inMemoryData.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
            inMemoryData.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
            inMemoryData.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
            inMemoryData.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
            inMemoryData.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));

            // creation of the schema
            StructField[] schemaFields = new StructField[]{
                    new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
            };
            StructType schema = new StructType(schemaFields);

            Dataset<Row> dataFrame = sparkSession.createDataFrame(inMemoryData, schema);
            dataFrame.createOrReplaceTempView("logging_table");

            // without giving an alias for the "date_format" column, it prints the implicit cast done: date_format(CAST(datetime AS TIMESTAMP)
            Dataset<Row> results = sparkSession.sql("select level, date_format(datetime, 'yyyy') from logging_table order by level");
            results.show();

            results = sparkSession.sql("select level, date_format(datetime, 'MMMM') as month from logging_table order by level");
            results.show();
        }
    }

    private static void lecture66MultipleGrouping() {

        System.out.println("---- lecture66MultipleGrouping");

        try (SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            List<Row> inMemoryData = new ArrayList<>();
            inMemoryData.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
            inMemoryData.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
            inMemoryData.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
            inMemoryData.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
            inMemoryData.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));

            // creation of the schema
            StructField[] schemaFields = new StructField[]{
                    new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
            };
            StructType schema = new StructType(schemaFields);

            Dataset<Row> dataFrame = sparkSession.createDataFrame(inMemoryData, schema);
            dataFrame.createOrReplaceTempView("logging_table");

            Dataset<Row> results = sparkSession.sql("select level, date_format(datetime, 'MMMM') as month from logging_table");
            results.show();

            results.createOrReplaceTempView("logging_table");
            Dataset<Row> resultsMultipleGrouping = sparkSession.sql("select level, month, count(1) as total from logging_table group by level, month");
            resultsMultipleGrouping.show();
        }
    }

    private static void lecture66MultipleGroupingBigLog() {

        System.out.println("---- lecture66MultipleGroupingBigLog");

        try (SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            // creation of the schema
            StructField[] schemaFields = new StructField[]{
                    new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
            };
            StructType schema = new StructType(schemaFields);

            Dataset<Row> dataFrame = sparkSession.read().option("header", true).csv("src/main/resources/biglog.txt");
            dataFrame.createOrReplaceTempView("logging_table");

            Dataset<Row> results = sparkSession.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total from logging_table group by level, month");
            results.show();

            results.createOrReplaceTempView("total_results");
            Dataset<Row> resultsTotal = sparkSession.sql("select sum(total) from total_results");
            resultsTotal.show();
        }
    }

    private static void lecture67Ordering() {

        System.out.println("---- lecture67Ordering");

        try (SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            // creation of the schema
            StructField[] schemaFields = new StructField[]{
                    new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
            };
            StructType schema = new StructType(schemaFields);

            Dataset<Row> dataFrame = sparkSession.read().option("header", true).csv("src/main/resources/biglog.txt");
            dataFrame.createOrReplaceTempView("logging_table");

            Dataset<Row> results = sparkSession.sql("select " +
                    "level, date_format(datetime, 'MMMM') as month, first(date_format(datetime, 'M')) as monthnum, count(1) as total " +
                    "from logging_table " +
                    "group by level, month " +
                    "order by monthnum");
            results.show(30);

            // cast monthnum to int to avoid an alphabetic sort
            results = sparkSession.sql("select " +
                    "level, date_format(datetime, 'MMMM') as month, cast(first(date_format(datetime, 'M')) as int) as monthnum, count(1) as total " +
                    "from logging_table " +
                    "group by level, month " +
                    "order by monthnum");
            results.show(30);

            // if we just want to sort our date, we can avoid to declare our extra column, and, instead, compute it in the order clause
            results = sparkSession.sql("select " +
                    "level, date_format(datetime, 'MMMM') as month, count(1) as total " +
                    "from logging_table " +
                    "group by level, month " +
                    "order by cast(first(date_format(datetime, 'M')) as int), level");
            results.show(100);
        }
    }

    private static void lecture68DataFrameAPI() {

        System.out.println("---- lecture68DataFrameAPI");

        try (SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            // creation of the schema
            StructField[] schemaFields = new StructField[]{
                    new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
            };
            StructType schema = new StructType(schemaFields);

            Dataset<Row> dataFrame = sparkSession.read().option("header", true).csv("src/main/resources/biglog.txt");

/*            dataFrame.createOrReplaceTempView("logging_table");
            Dataset<Row> results = sparkSession.sql("select " +
                    "level, date_format(datetime, 'MMMM') as month, count(1) as total " +
                    "from logging_table " +
                    "group by level, month " +
                    "order by cast(first(date_format(datetime, 'M')) as int), level");*/

            // "half way" method of using the DataFrame API: using parts of Java and SQL API
            Dataset<Row> resultsHalfWay = dataFrame.selectExpr("level", "date_format(datetime, 'MMMM') as month");
            resultsHalfWay.show(30);

            // full Java API way to create our statement
            Dataset<Row> resultsJava = dataFrame.select(col("level"), date_format(col("datetime"), "MMMM").as("month"));
            resultsJava.show(30);
        }
    }

    private static void lecture69DataFrameGrouping() {

        System.out.println("---- lecture69DataFrameGrouping");

        try (SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/")
                .getOrCreate()) {

            // creation of the schema
            StructField[] schemaFields = new StructField[]{
                    new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
            };
            StructType schema = new StructType(schemaFields);

            Dataset<Row> dataFrame = sparkSession.read().option("header", true).csv("src/main/resources/biglog.txt");

            // groupBy statement with DataFrame API
            Dataset<Row> results = dataFrame.select(
                    col("level"),
                    date_format(col("datetime"), "MMMM").as("month"),
                    date_format(col("datetime"), "M").cast(DataTypes.IntegerType).as("monthnum")
            );

            // Any column in the select BUT NOT IN THE groupBy will be dropped by the process
            // We MUST add "monthnum" here in the groupBy to be able to use it in the next coming orderBy
            RelationalGroupedDataset resultsGroup = results.groupBy(col("level"), col("month"), col("monthnum"));
            Dataset<Row> resultsGroupCount = resultsGroup.count();
            Dataset<Row> resultsGroupCountOrder = resultsGroupCount.orderBy(col("monthnum"), col("level"));
            // Now that the ordering is done, we can drop the "only for process" monthnum column
            Dataset<Row> resultsGroupCountOrderFinal = resultsGroupCountOrder.drop(col("monthnum"));
            resultsGroupCountOrderFinal.show(30);

        }
    }

}
