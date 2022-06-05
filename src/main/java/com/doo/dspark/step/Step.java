package com.doo.dspark.step;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

import java.util.HashMap;

public class Step {


        public static void main(String[] args) {
            // $example on$
            SparkConf sparkConf = new SparkConf().setAppName("JavaRandomForestClassificationExample")
                    .setMaster("local[*]").set("spark.executor.memory","2g");
            JavaSparkContext jsc = new JavaSparkContext(sparkConf);
            // Load and parse the data file.
            String datapath = "/Users/doo/sample/spark/data/mllib/sample_multiclass_classification_data.txt";
            JavaRDD data = MLUtils.loadLibSVMFile(jsc.sc(), datapath).toJavaRDD();
            // Split the data into training and test sets (30% held out for testing)
            JavaRDD[] splits = data.randomSplit(new double[]{0.7, 0.3});
            JavaRDD trainingData = splits[0];
            JavaRDD testData = splits[1];

            // Train a RandomForest model.
            // Empty categoricalFeaturesInfo indicates all features are continuous.
            Integer numClasses = 3;
            HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
            Integer numTrees = 55; // Use more in practice.
            String featureSubsetStrategy = "auto"; // Let the algorithm choose.
            String impurity = "gini";
            Integer maxDepth = 5;
            Integer maxBins = 32;
            Integer seed = 12345;

            final RandomForestModel model = RandomForest.trainClassifier(trainingData, numClasses,
                    categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
                    seed);

            // Evaluate model on test instances and compute test error
            JavaPairRDD<Double, Double> predictionAndLabel =
                    testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                        @Override
                        public Tuple2<Double, Double> call(LabeledPoint p) {
                            return new Tuple2<>(model.predict(p.features()), p.label());
                        }
                    });
            Double testErr =
                    1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
                        @Override
                        public Boolean call(Tuple2<Double, Double> pl) {
                            return !pl._1().equals(pl._2());
                        }
                    }).count() / testData.count();
            System.out.println("Test Error: " + testErr);
            System.out.println("Learned classification forest model:\n" + model.toDebugString());

            // Save and load model
            model.save(jsc.sc(), "target/tmp/myRandomForestClassificationModel");
            RandomForestModel sameModel = RandomForestModel.load(jsc.sc(),
                    "target/tmp/myRandomForestClassificationModel");
            // $example off$

            jsc.stop();


        }




}
