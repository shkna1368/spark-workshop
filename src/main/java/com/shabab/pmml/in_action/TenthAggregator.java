package com.shabab.pmml.in_action;

import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

public class TenthAggregator {

    public static void start(){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Restaurent").getOrCreate();

        // Reads a CSV file with header, called orders.csv, stores it in a
        // dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("data/orders.csv");

        // Calculating the orders info using the dataframe API
        Dataset<Row> apiDf = df
                .groupBy(col("firstName"), col("lastName"), col("state"))
                .agg(sum("quantity"), sum("revenue"), avg("revenue"));
        apiDf.show(20);


        // Calculating the orders info using SparkSQL
        df.createOrReplaceTempView("orders");
        String sqlStatement = "SELECT " +
                "    firstName, " +
                "    lastName, " +
                "    state, " +
                "    SUM(quantity), " +
                "    SUM(revenue), " +
                "    AVG(revenue) " +
                "  FROM orders " +
                "  GROUP BY firstName, lastName, state";
        Dataset<Row> sqlDf = spark.sql(sqlStatement);
        sqlDf.show(20);
        spark.stop();

    }
}
