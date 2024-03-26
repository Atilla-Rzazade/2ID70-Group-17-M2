package org.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class question4 {

    public static void solution(SparkSession spark, JavaRDD<String> eventsRDD, JavaRDD<String> eventsTypesRDD) {

        // Define the PairFunctions
        PairFunction<String, Integer, Tuple2<Integer, Integer>> eventsPairFunction = line -> {
            String[] parts = line.split(",");
            return new Tuple2<>(Integer.parseInt(parts[2]),
                    new Tuple2<>(Integer.parseInt(parts[0]), Integer.parseInt(parts[1])));
        };

        PairFunction<String, Integer, Integer> eventTypesPairFunction = line -> {
            String[] parts = line.split(",");
            return new Tuple2<>(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
        };

        // Generate JavaPairRDDs
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> events = eventsRDD.mapToPair(eventsPairFunction);
        JavaPairRDD<Integer, Integer> eventTypes = eventsTypesRDD.mapToPair(eventTypesPairFunction);

        // Join the two JavaPairRDDs
        JavaPairRDD<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>> joined = events.join(eventTypes);

        // Group by seriesid
        JavaPairRDD<Object, Iterable<Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>>>> grouped = joined
                .groupBy(t -> t._2._1._1);

        
        FlatMapFunction<Tuple2<Object, Iterable<Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>>>>, List<Integer>> generateSequences = new FlatMapFunction<Tuple2<Object, Iterable<Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>>>>, List<Integer>>() {
            @Override
            public Iterator<List<Integer>> call(
                    Tuple2<Object, Iterable<Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>>>> t) {
                        List<Tuple2<Tuple2<Integer, Integer>, Integer>> eventsList = new ArrayList<>();
                        t._2.forEach(tuple -> eventsList.add(tuple._2)); // Extract the second element of the tuple
                        eventsList.sort(Comparator.comparing(e -> e._1._2)); // Sort by timestamp
                        List<List<Integer>> sequences = new ArrayList<>();
                        for (int i = 0; i <= eventsList.size() - 5; i++) {
                            List<Integer> sequence = new ArrayList<>();
                            for (int j = i; j < i + 5; j++) {
                                sequence.add(eventsList.get(j)._2); // Add eventtypeid
                            }
                            sequences.add(sequence);
                        }
                        return sequences.iterator();
                    }
        };

        // Generate sequences
        JavaRDD<List<Integer>> sequences = grouped.flatMap(generateSequences);

        // Count occurrences
        JavaPairRDD<List<Integer>, Integer> counted = sequences.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey(Integer::sum);

        // Filter λ-frequent sequences
        JavaPairRDD<List<Integer>, Integer> frequent = counted.filter(t -> t._2 >= 5);
        long q4 = frequent.keys().distinct().count();
        System.out.println(">> [q4: " + q4 + "]");
    }
}
