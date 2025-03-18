package com.shabab.pmml.in_action;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FirstBookReader {


    public static void start(){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Simple Application").getOrCreate();
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("data/books.csv");
        df.show(5);
        spark.stop();
    }
}
