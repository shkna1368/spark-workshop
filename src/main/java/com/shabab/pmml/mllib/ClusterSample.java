package com.shabab.pmml.mllib;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
public class ClusterSample {

    public static void start(){

        SparkSession spark = SparkSession.builder().master("local[*]").appName("CSV to DB").getOrCreate();

        Dataset<Row> dataset = spark.read().format("libsvm").load("data/sample_kmeans_data.txt");

// Trains a k-means model.
        KMeans kmeans = new KMeans().setK(2).setSeed(1L);
        KMeansModel model = kmeans.fit(dataset);

// Make predictions
        Dataset<Row> predictions = model.transform(dataset);

// Evaluate clustering by computing Silhouette score
        ClusteringEvaluator evaluator = new ClusteringEvaluator();

        double silhouette = evaluator.evaluate(predictions);
        System.out.println("Silhouette with squared euclidean distance = " + silhouette);

// Shows the result.
        Vector[] centers = model.clusterCenters();
        System.out.println("Cluster Centers: ");
        for (Vector center: centers) {
            System.out.println(center);
        }
        spark.stop();

    }
}
