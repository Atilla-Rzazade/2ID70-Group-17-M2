package org.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class question4_test {
    public static void solution(SparkSession spark, JavaRDD<String> eventsRDD,JavaRDD<String> eventTypesRDD) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaPairRDD<String, String> seriesPairRDD = eventsRDD.mapToPair(line -> {
            String[] parts = line.split(",");
            return new Tuple2<>(parts[0], parts[2]); // (seriesID, eventID)
        });

        JavaPairRDD<String, String> eventTypesPairRDD = eventTypesRDD.mapToPair(line -> {
            String[] parts = line.split(",");
            return new Tuple2<>(parts[0], parts[1]); // (eventID, eventTypeID)
        });


        JavaPairRDD<String, Tuple2<String, String>> joinedRDD = seriesPairRDD.join(eventTypesPairRDD);
    
        JavaPairRDD<String, Iterable<String>> seriesToEventTypes = joinedRDD
        .mapToPair(tuple -> new Tuple2<>(tuple._1, tuple._2._2)) // Get a pair of (seriesID, eventTypeID)
        .groupByKey(); // Group by seriesID

        JavaRDD<String> sequences = seriesToEventTypes
                .flatMap(seriesEventTypes -> {
                    List<String> eventTypesList = new ArrayList<>();
                    seriesEventTypes._2.forEach(eventTypesList::add);
                    // Assuming eventTypesList is in the correct order; if not, sort it based on your criteria

                    List<String> seqs = new ArrayList<>();
                    for (int i = 0; i <= eventTypesList.size() - 5; i++) {
                        // Generate sequences of 5
                        String seq = String.join(",", eventTypesList.subList(i, i + 5));
                        seqs.add(seq);
                    }
                    return seqs.iterator();
                });


        long q4 = sequences.distinct().count();

        System.out.println(">> [q4: " + q4 + "]");
    }
}