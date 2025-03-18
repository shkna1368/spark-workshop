package com.shabab.pmml.in_action;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class SecondAuthorReader {

    public static void start(){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("CSV to DB").getOrCreate();


        // Step 1: Ingestion
        // ---------

        // Reads a CSV file with header, called authors.csv, stores it in a
        // dataframe
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/authors.csv");

        // Step 2: Transform
        // ---------

        // Creates a new column called "name" as the concatenation of lname, a
        // virtual column containing ", " and the fname column
        df = df.withColumn(
                "name",
                concat(df.col("lname"), lit(", "), df.col("fname")));

        // Step 3: Save
        // ---------

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

        // Write in a table called full_name_table
        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "fulls_names_tabls", prop);

        System.out.println("Process complete");

        spark.stop();

    }
}
