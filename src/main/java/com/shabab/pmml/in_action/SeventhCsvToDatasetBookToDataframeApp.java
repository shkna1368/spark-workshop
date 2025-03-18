package com.shabab.pmml.in_action;

import java.io.Serializable;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;

import java.io.Serializable;
import java.text.SimpleDateFormat;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SeventhCsvToDatasetBookToDataframeApp implements Serializable {
    private static final long serialVersionUID = -1L;


    class BookMapper implements MapFunction<Row, Book> {
        private static final long serialVersionUID = -2L;

        @Override
        public Book call(Row value) throws Exception {
            Book b = new Book();
            b.setId(value.getAs("id"));
            b.setAuthorId(value.getAs("authorId"));
            b.setLink(value.getAs("link"));
            b.setTitle(value.getAs("title"));

            // date case
            String dateAsString = value.getAs("releaseDate");
            if (dateAsString != null) {
                SimpleDateFormat parser = new SimpleDateFormat("M/d/yy");
                b.setReleaseDate(parser.parse(dateAsString));
            }
            return b;
        }
    }

    /**
     * It starts here
     *
     * @param args
     */


    /**
     * All the work is done here.
     */
    public  void start() {

        SparkSession spark = SparkSession.builder().master("local[*]").appName("CSV to DB").getOrCreate();

        String filename = "data/books.csv";
        Dataset<Row> df = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filename);

        System.out.println("*** Books ingested in a dataframe");
        df.show(5);
        df.printSchema();

        Dataset<Book> bookDs = df.map(
                new BookMapper(),
                Encoders.bean(Book.class));
        System.out.println("*** Books are now in a dataset of books");
        bookDs.show(5, 17);
        bookDs.printSchema();
        Dataset<Row> df2 = bookDs.toDF();
        df2 = df2.withColumn(
                "releaseDateAsString",
                concat(
                        expr("releaseDate.year + 1900"), lit("-"),
                        expr("releaseDate.month + 1"), lit("-"),
                        df2.col("releaseDate.date")));

        df2.show(5);

        spark.stop();
    }

}
