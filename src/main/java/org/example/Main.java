package org.example;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.*;
import scala.Tuple2;


public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getRootLogger().setLevel(Level.OFF);

        boolean onServer = false; // TODO: Set this to true if and only if building a JAR to run on the server

        SparkConf conf = new SparkConf()
                .setAppName(Main.class.getName());
        if (!onServer) conf = conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession
                .builder()
                .appName("2ID70")
                .getOrCreate();

        String eventsPath = (onServer) ? "/events.csv" : "./src/main/resources/smallDataset/events.csv";
        String eventTypesPath = (onServer) ? "/eventtypes.csv" : "./src/main/resources/smallDataset/eventtypes.csv";
        JavaRDD<String> eventsRDD = spark.read().textFile(eventsPath).javaRDD();
        JavaRDD<String> eventTypesRDD = spark.read().textFile(eventTypesPath).javaRDD();

        Tuple2<JavaRDD<String>, JavaRDD<String>> cleaned = question1.solution(spark, eventsRDD, eventTypesRDD);
        JavaRDD<String> df1 = cleaned._1();
        JavaRDD<String> df2 = cleaned._2();
        //question2.solution(spark, df1, df2);
        question3_boris.solution(spark, df1);
        //question4.solution(spark, df1, df2);
        question4_test.solution(spark, df1, df2);
    }
}