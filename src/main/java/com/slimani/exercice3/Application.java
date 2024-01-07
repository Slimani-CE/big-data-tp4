package com.slimani.exercice3;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class Application {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("Spark with CSV Files").master("local[*]").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());

        // meteo.csv file
        ss.read().csv("src/main/resources/2020.csv");
    }
}