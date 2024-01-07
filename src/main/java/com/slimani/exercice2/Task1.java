package com.slimani.exercice2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class Task1 {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("Spark SQL with Text Files").master("local[*]").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());

        JavaRDD<String> dataRDD;

        // Read the ventes.txt file
        // If we're running inside hadoop, we get the path of the file from the command line arguments
        if (args.length > 0) {
            dataRDD = sc.textFile(args[0]);
        } else {
            dataRDD = sc.textFile("src/main/resources/ventes.txt");
        }

        // Filter the data to get only 'ville' and 'prix' columns
        JavaRDD<String> filteredDataRDD = dataRDD.map(line -> {
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

        sc.stop(); // Stop the SparkContext
    }
}
