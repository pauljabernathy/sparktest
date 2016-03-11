/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparktest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.*;

/**
 *
 * @author pabernathy
 */
public class UseSparkSQL {
    
    private static SparkConf conf;
    private static JavaSparkContext sc;
    private static HiveContext hc;
    static {
        System.setProperty("spark.master", "http://localhost:4040");
        conf = new SparkConf().setAppName("wordCount");
        sc = new JavaSparkContext("local", "wordcount", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        HiveContext hc = new HiveContext(sc);
    }
    
    public static void main(String[] args) {
        UseSparkSQL.readCSV();
    }
    
    public static void readCSV() {
        DataFrame df = hc.read().format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").load("flowers.csv");
        df.select("flower", "rose").show();
    }
}
