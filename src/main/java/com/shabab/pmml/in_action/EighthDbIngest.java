package com.shabab.pmml.in_action;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class EighthDbIngest {

    public static void start(){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("CSV to DB").getOrCreate();



        // The connection URL, assuming your PostgreSQL instance runs locally on
        // the
        // default port, and the database we use is "spark_labs"
        String dbConnectionUrl = "jdbc:postgresql://localhost/spark_labs";

        // Properties to connect to the database, the JDBC driver is part of our
        // pom.xml
        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user", "postgres");
        prop.setProperty("password", "postgres");

        Dataset<Row> df = spark.read().jdbc(
                dbConnectionUrl,
                "ch02", prop);
//        Dataset<Row> df = spark.read().jdbc(
//                "jdbc:mysql://localhost:3306/sakila",
//                "(" + sqlQuery + ") film_alias",
//                props);
        df = df.orderBy(df.col("lname"));
        df.show(5);
        df.printSchema();

        System.out.println("Process complete");

        spark.stop();
    }
}
