package com.shabab.pmml.mllib;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;

public class CollaborativeFilteringRecommender {
    public static void start(){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("CSV to DB").getOrCreate();


        JavaRDD<Rating> ratingsRDD = spark
                .read().textFile("data/sample_movielens_ratings.txt").javaRDD()
                .map(Rating::parseRating);
        Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);
        Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];

// Build the recommendation model using ALS on the training data
        ALS als = new ALS()
                .setMaxIter(5)
                .setRegParam(0.01)
                .setUserCol("userId")
                .setItemCol("movieId")
                .setRatingCol("rating");
        ALSModel model = als.fit(training);

// Evaluate the model by computing the RMSE on the test data
// Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
        model.setColdStartStrategy("drop");
        Dataset<Row> predictions = model.transform(test);

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("rating")
                .setPredictionCol("prediction");
        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root-mean-square error = " + rmse);

// Generate top 10 movie recommendations for each user
        Dataset<Row> userRecs = model.recommendForAllUsers(10);
// Generate top 10 user recommendations for each movie
        Dataset<Row> movieRecs = model.recommendForAllItems(10);

// Generate top 10 movie recommendations for a specified set of users
        Dataset<Row> users = ratings.select(als.getUserCol()).distinct().limit(3);
      users.show(10);
        Dataset<Row> userSubsetRecs = model.recommendForUserSubset(users, 10);
        userSubsetRecs.show(10);

        // Generate top 10 user recommendations for a specified set of movies
        Dataset<Row> movies = ratings.select(als.getItemCol()).distinct().limit(3);
       movies.show(10);
        Dataset<Row> movieSubSetRecs = model.recommendForItemSubset(movies, 10);
        movieSubSetRecs.show(10,false);
        spark.stop();
    }

}
