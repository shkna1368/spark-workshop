package com.shabab.pmml.mllib;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
public class NaiveBayesSample {

    public static void start(){

        SparkSession spark = SparkSession.builder().master("local[*]").appName("CSV to DB").getOrCreate();

        Dataset<Row> dataFrame =
                spark.read().format("libsvm").load("data/sample_libsvm_data.txt");
// Split the data into train and test
        Dataset<Row>[] splits = dataFrame.randomSplit(new double[]{0.6, 0.4}, 1234L);
        Dataset<Row> train = splits[0];
        Dataset<Row> test = splits[1];

// create the trainer and set its parameters
        NaiveBayes nb = new NaiveBayes();

// train the model
        NaiveBayesModel model = nb.fit(train);

// Select example rows to display.
        Dataset<Row> predictions = model.transform(test);
        predictions.show();

// compute accuracy on the test set
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test set accuracy = " + accuracy);
    }
}
