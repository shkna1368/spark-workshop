package com.shabab.pmml.in_action;

import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
public class FourthJson {

    public static void start(){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("CSV to DB").getOrCreate();

// Reads a JSON file called Restaurants_in_Durham_County_NC.json, stores
        // it
        // in a dataframe
        Dataset<Row> df = spark.read().format("json")
                .load("data/Restaurants_in_Durham_County_NC.json");
        System.out.println("*** Right after ingestion");
        df.show(5);
        df.printSchema();
        System.out.println("We have " + df.count() + " records.");

        df = df.withColumn("county", lit("Durham"))
                .withColumn("datasetId", df.col("fields.id"))
                .withColumn("name", df.col("fields.premise_name"))
                .withColumn("address1", df.col("fields.premise_address1"))
                .withColumn("address2", df.col("fields.premise_address2"))
                .withColumn("city", df.col("fields.premise_city"))
                .withColumn("state", df.col("fields.premise_state"))
                .withColumn("zip", df.col("fields.premise_zip"))
                .withColumn("tel", df.col("fields.premise_phone"))
                .withColumn("dateStart", df.col("fields.opening_date"))
                .withColumn("dateEnd", df.col("fields.closing_date"))
                .withColumn("type",
                        split(df.col("fields.type_description"), " - ").getItem(1))
                .withColumn("geoX", df.col("fields.geolocation").getItem(0))
                .withColumn("geoY", df.col("fields.geolocation").getItem(1));
        df = df.withColumn("id",
                concat(df.col("state"), lit("_"),
                        df.col("county"), lit("_"),
                        df.col("datasetId")));

        System.out.println("*** Dataframe transformed");
        df.show(5);
        df.printSchema();

        System.out.println("*** Looking at partitions");
        Partition[] partitions = df.rdd().partitions();
        int partitionCount = partitions.length;
        System.out.println("Partition count before repartition: " +
                partitionCount);

        df = df.repartition(4);
        System.out.println("Partition count after repartition: " +
                df.rdd().partitions().length);


        spark.stop();
    }
}
