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

public class question3 {
    
    private static final Pattern COMMA = Pattern.compile(" ");

    public static void solution(SparkSession spark, JavaRDD<String> eventsRDD) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaPairRDD<String, ArrayList<Tuple2<Integer, String>>> events = eventsRDD
            .mapToPair(s -> {
                String[] parts = s.split(",", 3);
                String key = parts[0];
                ArrayList<Tuple2<Integer, String>> list = new ArrayList<>(Arrays.asList(new Tuple2<Integer, String>(Integer.parseInt(parts[1]), parts[2])));
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
                for (int i = 0; i < list.size() - 2; i++) {
                    String seq = list.get(i)._2 + "," + list.get(i + 1)._2 + ","
                            + list.get(i + 2)._2;
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
        
        long q3 = RDDQ3.distinct().count();

        System.out.println(">> [q3: " + q3 + "]");
    }
}
