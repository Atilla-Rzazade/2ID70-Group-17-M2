package org.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class question4_test {
    public static void solution(SparkSession spark, JavaRDD<String> eventsRDD, JavaRDD<String> eventTypesRDD) {
        // Create a JavaSparkContext from the existing SparkContext
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // Transform the eventsRDD into a pair RDD where the key is the event ID and the
        // value is a list of tuples
        // Each tuple contains the timestamp and the event type
        JavaPairRDD<String, ArrayList<Tuple2<Integer, String>>> events = eventsRDD
                .mapToPair(s -> {
                    String[] parts = s.split(",", 5); // Split the string into parts
                    String key = parts[0]; // The key is the first part
                    ArrayList<Tuple2<Integer, String>> list = new ArrayList<>(
                            Arrays.asList(new Tuple2<Integer, String>(Integer.parseInt(parts[1]), parts[2]))); // The
                                                                                                               // value
                                                                                                               // is a
                                                                                                               // list
                                                                                                               // of
                                                                                                               // tuples
                    return new Tuple2<>(key, list); // Return a tuple with the key and the list
                });

        // Reduce the events RDD by key
        // This groups the tuples by their keys and combines the lists of tuples into a
        // single list
        JavaPairRDD<String, ArrayList<Tuple2<Integer, String>>> eventList = events
                .reduceByKey((list1, list2) -> {
                    list1.addAll(list2);
                    return list1;
                });

        // Sort the events in each list by their timestamps
        JavaPairRDD<String, ArrayList<Tuple2<Integer, String>>> sortedEvents = eventList
                .mapValues(list -> {
                    list.sort(Comparator.comparing(Tuple2::_1));
                    return list;
                });

        // Generate sequences of event types of size 5
        // Each sequence is a string of 5 event types separated by commas
        JavaPairRDD<String, ArrayList<String>> sequences = sortedEvents
                .mapValues(list -> {
                    ArrayList<String> seq = new ArrayList<>();
                    for (int i = 0; i < list.size() - 4; i++) {
                        seq.add(list.get(i)._2 + "," + list.get(i + 1)._2 + "," + list.get(i + 2)._2 + ","
                                + list.get(i + 3)._2 + "," + list.get(i + 4)._2);
                    }
                    return seq;
                });

        // Print out some of the sequences to check if they are correctly generated
        // [UNCOMMENT THE FOLLOWING FOR DEBUGGING PURPOSES]
        // sequences.take(10).forEach(System.out::println);

        // Transform the sequences RDD into a pair RDD where the key is the sequence and
        // the value is 1
        // This is done by creating a new pair for each sequence in the list of
        // sequences
        JavaPairRDD<String, Integer> flattenedSequences = sequences
                .flatMapToPair(s -> {
                    ArrayList<Tuple2<String, Integer>> result = new ArrayList<>();
                    for (String seq : s._2) { // For each sequence in the list
                        result.add(new Tuple2<>(seq, 1)); // Add a new pair with the sequence as the key and 1 as the
                                                          // value
                    }
                    return result.iterator(); // Return an iterator over the list of pairs
                });

        // Print out some of the sequence counts to check if they are correctly counted
        // [UNCOMMENT THE FOLLOWING FOR DEBUGGING PURPOSES]
        // flattenedSequences.take(10).forEach(System.out::println);

        // Reduce the flattenedSequences RDD by key
        // This groups the pairs by their keys (the sequences) and sums the values (the
        // counts)
        // The result is a new RDD where each sequence is paired with its total count
        JavaPairRDD<String, Integer> sequenceCounts = flattenedSequences
                .reduceByKey((i1, i2) -> i1 + i2);

        // Print out some of the sequence counts to check if any sequences occur 5 times
        // or more
        // [UNCOMMENT THE FOLLOWING FOR DEBUGGING PURPOSES]
        // sequenceCounts.take(10).forEach(System.out::println);

        JavaPairRDD<String, Integer> frequentSequences = sequenceCounts
        .filter(t -> t._2 >= 5);

        long q4 = frequentSequences
        .map(t -> t._1)
        .distinct()
        .count();


        // The following code is a modified version of the code above that allows you to check for smaller λ-frequency
        // JavaPairRDD<String, Integer> frequentSequences = sequenceCounts
        //         .filter(t -> t._2 >= 2); // Change this to adjust the requirement for a sequence to be considered
        //                                  // frequent

        // long q4 = frequentSequences
        //         .map(t -> t._1)
        //         .distinct()
        //         .count();

        System.out.println(">> [q4: " + q4 + "]");
    }
}