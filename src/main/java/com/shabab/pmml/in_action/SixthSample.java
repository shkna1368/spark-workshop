package com.shabab.pmml.in_action;

import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
public class SixthSample {


    public static void start(){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Restaurent").getOrCreate();
        String[] stringList =
                new String[] { "Jean", "Liz", "Pierre", "Lauric" };
        List<String> data = Arrays.asList(stringList);
        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
        ds.show();
        ds.printSchema();

        spark.stop();

    }




}
