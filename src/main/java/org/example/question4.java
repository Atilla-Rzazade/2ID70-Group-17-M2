package org.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class question4 {

    public static void solution(SparkSession spark, JavaRDD<String> eventsRDD, JavaRDD<String> eventsTypesRDD) {

        JavaPairRDD typeMap = eventsTypesRDD
            .mapToPair(s -> {
                String[] parts = s.split(",", 2);
                return new Tuple2<>(parts[0], parts[1]);
            });
        
        Map<String, String> eventMap = typeMap.collectAsMap();

        JavaPairRDD<String, ArrayList<Tuple2<Integer, String>>> events = eventsRDD
            .mapToPair(s -> {
                String[] parts = s.split(",", 3);
                String key = parts[0];
                String type = eventMap.get(parts[2]);
                ArrayList<Tuple2<Integer, String>> list = new ArrayList<>(Arrays.asList(new Tuple2<Integer, String>(Integer.parseInt(parts[1]), type)));
                return new Tuple2<>(key, list);
            });

        JavaPairRDD<String, ArrayList<Tuple2<Integer, String>>> eventList = events
            .reduceByKey((list1, list2)-> {
                list1.addAll(list2);
                return list1;
            });

        JavaPairRDD<String, ArrayList<Tuple2<Integer, String>>> sortedEvents = eventList
            .mapValues(list -> {
                list.sort(Comparator.comparing(Tuple2::_1));
                return list;
            });

        JavaPairRDD<String, ArrayList<String>> sequences = sortedEvents
            .mapValues(list -> {
                ArrayList<String> seqs = new ArrayList<>();
                for (int i = 0; i < list.size() - 4; i++) {
                    String seq = list.get(i)._2 + "," 
                            + list.get(i + 1)._2 + ","
                            + list.get(i + 2)._2 + ","
                            + list.get(i + 3)._2 + ","
                            + list.get(i + 4)._2;
                    seqs.add(seq);
                }
                return seqs;
            });

        JavaPairRDD<String, String> flattenedSequences = sequences
            .flatMapValues(list -> {
                return list.iterator();
            });

        JavaPairRDD<Tuple2<String, String>, Integer> sequenceCounts = flattenedSequences
            .mapToPair(sequence -> new Tuple2<>(sequence, 1))
            .reduceByKey((count1, count2) -> count1 + count2);

        JavaRDD<String> RDDQ3 = sequenceCounts
            .filter(sequence -> sequence._2() >= 5)
            .map(sequence -> sequence._1._2);
        
        long q4 = RDDQ3.distinct().count();

        System.out.println(">> [q4: " + q4 + "]");
    }
}
