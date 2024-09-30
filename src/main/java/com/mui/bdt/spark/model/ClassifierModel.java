package com.mui.bdt.spark.model;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class ClassifierModel {

    private static final String[] featureColumns = new String[]{
            "V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9",
            "V10", "V11", "V12", "V13", "V14", "V15", "V16", "V17",
            "V18", "V19", "V20", "V21", "V22", "V23", "V24", "V25",
            "V26", "V27", "V28", "Amount", "Time"
    };

    private static final VectorAssembler assembler = new VectorAssembler()
            .setInputCols(featureColumns)
            .setOutputCol("assembled_features");

    public static Dataset<Row> loadData() {
        SparkSession spark = SparkSession.builder()
                .appName("Fraud Detection with Random Forest")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> data = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("src/main/resources/input.csv");

        data = data.withColumn("Time", data.col("Time").cast(DataTypes.DoubleType))
                .withColumn("Amount", data.col("Amount").cast(DataTypes.DoubleType))
                .withColumn("Class", data.col("Class").cast(DataTypes.DoubleType));

        return data;
    }

    public static void train() throws Exception {
        Dataset<Row> data = loadData();

        Dataset<Row>[] splits = data.randomSplit(new double[]{0.8, 0.2}, 1234L);
        Dataset<Row> trainData = splits[0];
        Dataset<Row> testData = splits[1];

        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol("Class")
                .setFeaturesCol("assembled_features");

        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{assembler, rf});

        PipelineModel model = pipeline.fit(trainData);

        Dataset<Row> predictions = model.transform(testData);

        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
                .setLabelCol("Class")
                .setMetricName("areaUnderPR");

        double areaUnderPR = evaluator.evaluate(predictions);
        System.out.println("Area Under Precision-Recall Curve: " + areaUnderPR);

        model.save("src/main/java/com/mui/bdt/spark/model/random_forest_model");
    }
}
