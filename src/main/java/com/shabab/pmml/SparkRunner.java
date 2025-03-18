package com.shabab.pmml;


import com.shabab.pmml.in_action.*;
import com.shabab.pmml.mllib.ClusterSample;
import com.shabab.pmml.mllib.CollaborativeFilteringRecommender;
import com.shabab.pmml.mllib.DecisionTreeSample;
import com.shabab.pmml.mllib.NaiveBayesSample;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.*;


import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;


import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.call_udf;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;


import org.apache.spark.ml.stat.Summarizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;

import scala.collection.immutable.Seq;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkRunner {

    public static void main(String[] args) {
        //firstApp();
        //secondApp();
       // thirdApp();
       // FirstBookReader.start();
      //  SecondAuthorReader.start();
        //ThirdRestarent.start();
      //  FourthJson.start();
        //FifthSample.start();
       // SixthSample.start();
     //  new SeventhCsvToDatasetBookToDataframeApp().start();

//EighthDbIngest.start();
//NinthRecordTransformation.start();
//TenthAggregator.start();
       // NaiveBayesSample.start();
      //  DecisionTreeSample.start();
      //  ClusterSample.start();
        CollaborativeFilteringRecommender.start();

     //   String logFile = "README.md";
/*        // Setup Spark Configuration
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkJavaHelloWorld")
                .setMaster("local[*]");

        // Define Java Spark Context
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);*/


      //  SparkSession spark = SparkSession.builder().master("local[*]").appName("Simple Application").getOrCreate();

     //   callDefult(spark);

      //  callTara(spark);
//       Dataset<Row> logData = spark.read().textFile(logFile).cache().toDF();
//
//
//        long numAs = logData.filter((Row s) -> s.toString().contains("a")).count();
//        long numBs = logData.filter((Row s) -> s.toString().contains("b")).count();
//
//        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
//
//

//
//        //sample2
//        var df = spark.read().json("people.json");
//
//        df.show();
//
//        df.select("name").show();
//
//        df.select(col("name"), col("age").plus(1)).show();
//
//        df.filter(col("age").gt(21)).show();
//        df.groupBy("age").count().show();



/*


        //sql
        System.out.println("---------------sql---------------------");
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();*/

//////////////////////ml




         //   spark.stop();
        }

        public static void firstApp(){
            String logFile = "README.md";
            SparkSession spark = SparkSession.builder().master("local[*]").appName("Simple Application").getOrCreate();
       Dataset<Row> logData = spark.read().textFile(logFile).cache().toDF();


        long numAs = logData.filter((Row s) -> s.toString().contains("a")).count();
        long numBs = logData.filter((Row s) -> s.toString().contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

            spark.stop();

        }
  public static void secondApp(){


      SparkSession spark = SparkSession.builder().master("local[*]").appName("Simple Application").getOrCreate();
      var df = spark.read().json("people.json");

      df.show();
//
      df.select("name").show();
//
      df.select(col("name"), col("age").plus(1)).show();
//
      df.filter(col("age").gt(21)).show();
      df.groupBy("age").count().show();

            spark.stop();

        }

         public static void thirdApp(){

      SparkSession spark = SparkSession.builder().master("local[*]").appName("Simple Application").getOrCreate();
      var df = spark.read().json("people.json");

    //  df.show();

             df.createOrReplaceTempView("people");

             Dataset<Row> sqlDF = spark.sql("SELECT * FROM people ");
             sqlDF.show();

            spark.stop();

        }



    public static void callTara(SparkSession spark){


        Dataset<Row> xTrain = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("x_train.csv");
xTrain.show(20);
        Dataset<Row> yTrain = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("y_train.csv");
        yTrain.show(20);

        // Load testing data
        Dataset<Row> xTest = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("x_test.csv");

        Dataset<Row> yTest = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("y_test.csv");

        // Merge x_train and y_train for model training
        Dataset<Row> trainingData = xTrain.withColumn("label", yTrain.col("n_interactions"));

        // Format features into a single vector
        String[] featureColumns = xTrain.columns();
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(featureColumns)
                .setOutputCol("features");
        Dataset<Row> trainingDataWithFeatures = assembler.transform(trainingData).select("label", "features");

        // Train Random Forest model
        RandomForestRegressor rf = new RandomForestRegressor()
                .setLabelCol("label")
                .setFeaturesCol("features");

        RandomForestRegressionModel model = rf.fit(trainingDataWithFeatures);

        // Prepare test data
        Dataset<Row> testData = xTest.withColumn("label", yTest.col("n_interactions"));
        Dataset<Row> testDataWithFeatures = assembler.transform(testData).select("label", "features");

        // Make predictions
        Dataset<Row> predictions = model.transform(testDataWithFeatures);

        // Evaluate the model
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("rmse");

        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);




    }




        public static void callDefult(SparkSession spark){

            // Load and parse the data file, converting it to a DataFrame.
            Dataset<Row> data = spark.read().format("libsvm").load("sample_libsvm_data.txt");
            //  Dataset<Row> data = spark.read().format("libsvm").load("X_train - Copy.csv");
            //   Dataset<Row> data = spark.read().option("delimiter", ",").load("X_train - Copy.csv");
            // Dataset<Row> data = spark.read().option("header", "true").csv("X_train.csv");
            data.printSchema();
//data.show(50);

            // data.select("total_purchases").show(20);
// Index labels, adding metadata to the label column.
// Fit on whole dataset to include all labels in index.
            StringIndexerModel labelIndexer = new StringIndexer()
                    .setInputCol("label")
                    .setOutputCol("indexedLabel")
                    .fit(data);
// Automatically identify categorical features, and index them.
// Set maxCategories so features with > 4 distinct values are treated as continuous.

            VectorIndexerModel featureIndexer = new VectorIndexer()
                    .setInputCol("features")
                    .setOutputCol("indexedFeatures")
                    .setMaxCategories(4)
                    .fit(data);

// Split the data into training and test sets (30% held out for testing)
            Dataset<Row>[] splits = data.randomSplit(new double[] {0.7, 0.3});
            Dataset<Row> trainingData = splits[0];
            Dataset<Row> testData = splits[1];

// Train a RandomForest model.
            RandomForestClassifier rf = new RandomForestClassifier()
                    .setLabelCol("indexedLabel")
                    .setFeaturesCol("indexedFeatures");

// Convert indexed labels back to original labels.
            IndexToString labelConverter = new IndexToString()
                    .setInputCol("prediction")
                    .setOutputCol("predictedLabel")
                    .setLabels(labelIndexer.labelsArray()[0]);

// Chain indexers and forest in a Pipeline
            Pipeline pipeline = new Pipeline()
                    .setStages(new PipelineStage[] {labelIndexer, featureIndexer, rf, labelConverter});

// Train model. This also runs the indexers.
            PipelineModel model = pipeline.fit(trainingData);

// Make predictions.
            Dataset<Row> predictions = model.transform(testData);

// Select example rows to display.
            predictions.select("predictedLabel", "label", "features").show(50);



//Select (prediction, true label) and compute test error
            MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                    .setLabelCol("indexedLabel")
                    .setPredictionCol("prediction")
                    .setMetricName("accuracy");
            double accuracy = evaluator.evaluate(predictions);
            System.out.println("Test Error = " + (1.0 - accuracy));




            RandomForestClassificationModel rfModel = (RandomForestClassificationModel)(model.stages()[2]);
            System.out.println("Learned classification forest model:\n" + rfModel.toDebugString());
        }
    }
