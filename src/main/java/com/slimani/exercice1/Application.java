package com.slimani.exercice1;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Application {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("Spark RDDs").master("local[*]").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());

        Dataset<Row> students;

        // If we're running inside hadoop, we get the path of the file from the command line arguments
        if (args.length > 0) {
            students = ss
                    .read()
                    .option("multiline", true)
                    .json(args[0]);
        } else {
            students = ss
                    .read()
                    .option("multiline", true)
                    .json("src/main/resources/students.json");
        }

        List<String> studentNames = students
                .select("name")
                .toJavaRDD().map(row -> row.getString(0)).collect();

        // RDD1: Creating the initial RDD by parallelizing the student names
        JavaRDD<String> rdd1 = sc.parallelize(studentNames);

        // RDD2: Transforming RDD1 using flatMap
        JavaRDD<String> rdd2 = rdd1.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        // RDD3 & RDD4: Filtering rdd2
        JavaRDD<String> rdd3 = rdd2.filter(s -> s.startsWith("A"));
        JavaRDD<String> rdd4 = rdd2.filter(s -> s.startsWith("B"));

        // RDD6: Union of RDD3 and RDD4
        JavaRDD<String> rdd6 = rdd3.union(rdd4);

        // RDD8: Performing map and reduceByKey operations on RDD6
        JavaRDD<String> rdd8 = rdd6.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((a, b) -> a + b)
                .map(pair -> pair._1() + ": " + pair._2());

        // RDD5: Filtering rdd2
        JavaRDD<String> rdd5 = rdd2.filter(s -> s.startsWith("C"));

        // RDD7: Performing map and reduceByKey operations on RDD5
        JavaRDD<String> rdd7 = rdd5.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((a, b) -> a + b)
                .map(pair -> pair._1() + ": " + pair._2());

        // RDD9: Union of RDD8 and RDD7
        JavaRDD<String> rdd9 = rdd8.union(rdd7);

        // RDD10: Sorting RDD9
        JavaRDD<String> rdd10 = rdd9.sortBy(s -> s, true, 1);

        // Action: Printing RDD10
        if (args.length > 1) {
            rdd10.saveAsTextFile(args[1]);
        } else {
            rdd10.collect().forEach(System.out::println);
        }

        sc.stop(); // Stop the SparkContext
    }
}
