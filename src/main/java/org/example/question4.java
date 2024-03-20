package org.example;

import java.util.ArrayList;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class question4 {

    public static void solution(SparkSession spark, JavaRDD<String> eventsRDD, JavaRDD<String> eventsTypesRDD) {
        
        JavaPairRDD<String, String> seriesPairRDD = eventsRDD.mapToPair(line -> {
            String[] parts = line.split(",");
            return new Tuple2<>(parts[0], parts[2]); // (seriesID, eventID)
        });

        JavaPairRDD<String, String> eventTypesPairRDD = eventsTypesRDD.mapToPair(line -> {
            String[] parts = line.split(",");
            return new Tuple2<>(parts[0], parts[1]); // (eventID, eventTypeID)
        });

        // Join Series and EventTypes RDDs based on eventID
        JavaPairRDD<String, Tuple2<String, String>> joinedRDD = seriesPairRDD.join(eventTypesPairRDD);

        // Group by SeriesID and create sequences of event types
        JavaPairRDD<String, Iterable<String>> groupedRDD = joinedRDD.groupByKey().mapValues(events -> {
            ArrayList<String> eventTypes = new ArrayList<>();
            for (Tuple2<String, String> event : events) {
                eventTypes.add(event._2());
            }
            return eventTypes;
        });

        // Filter λ-frequent sequences
        int lambda = 5; // λ = s = 5

        JavaPairRDD<String, Iterable<String>> frequentSequencesRDD = groupedRDD.filter(pair -> {
            Iterable<String> eventTypes = pair._2();
            ArrayList<String> eventTypesList = new ArrayList<>();
            eventTypes.forEach(eventTypesList::add);
            return eventTypesList.size() >= lambda;
        });

        long q4 = frequentSequencesRDD.distinct().count();
        System.out.println(">> [q4: " + q4 + "]");
    }
}
