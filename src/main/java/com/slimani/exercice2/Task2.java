package com.slimani.exercice2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class Task2 {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("Spark SQL with Text Files").master("local[*]").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());

        String date;
        JavaRDD<String> dataRDD;

        if (args.length > 0) {
            date = args[2];

            // Read the ventes.txt file
            dataRDD = sc.textFile(args[0]);
        } else {
            date = "2019";

            // Read the ventes.txt file
            dataRDD = sc.textFile("src/main/resources/ventes.txt");
        }

        // Filter the data to get only 'ville' and 'prix' for a given 'date'
        JavaRDD<String> filteredDataRDD = dataRDD.filter(line -> line.startsWith(date)).map(line -> {
            String[] columns = line.split(" ");
            return columns[1] + " " + columns[3];
        });

        // Create pairs of (ville, prix)
        JavaPairRDD<String, Double> pairsRDD = filteredDataRDD.mapToPair(line -> {
            String[] columns = line.split(" ");
            return new Tuple2<>(columns[0], Double.parseDouble(columns[1]));
        });

        // Reduce by key to get the total price for each city
        JavaPairRDD<String, Double> reducedRDD = pairsRDD.reduceByKey((a, b) -> a + b);

        // Sort the data by price
        JavaPairRDD<Double, String> sortedRDD = reducedRDD.mapToPair(pair -> new scala.Tuple2<>(pair._2(), pair._1()))
                .sortByKey(false);

        // Print the result
        if (args.length > 0) {
            sortedRDD.saveAsTextFile(args[1]);
        } else {
            sortedRDD.collect().forEach((pair) -> System.out.println(pair._2() + ": " + pair._1()));
        }
    }
}
