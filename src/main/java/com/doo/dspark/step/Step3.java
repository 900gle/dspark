package com.doo.dspark.step;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Step3 {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "/usr/local/Cellar/hadoop/3.3.3/bin");

        SparkSession spark = SparkSession.builder()
                .appName("TitanicSurvive")
                .config("spark.eventLog.enabled", "false")
                .master("local[*]")
                .config("spark.executor.memory", "1g")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        String path = "/Users/doo/project/dspark/test.json";




        Dataset<Row> peopleDF =
                spark.read().format("json").load(path);

        peopleDF.select("*").show();

        peopleDF.select("*").write().format("parquet").save("namesAndAges.parquet");
//
//        Dataset<Row> df1 = spark.read().text(path);


//        df1.show();




//
////        String path = "/Users/doo/project/dspark/test.json";
////
////        Dataset<Row> df1 = spark.read().text(path);
////        df1.show();
//
//
//        Dataset<Row> peopleDF =
//                spark.read().format("json").load("/Users/doo/project/dspark/products.json");
//        peopleDF.select("name").write().format("parquet").save("namesAndAges.parquet");

//        Dataset<Row> peopleDFCsv = spark.read().format("csv")
//                .option("sep", ";")
//                .option("inferSchema", "true")
//                .option("header", "true")
//                .load("/Users/doo/project/dspark/products.json");
//        Dataset<Row> df = spark.read().json("/Users/doo/project/dspark/products.json");

//        df.select("*").limit(1).show();
// Displays the content of the DataFrame to stdout
//        df.show();

//        df.select("*").show();


    }
}
