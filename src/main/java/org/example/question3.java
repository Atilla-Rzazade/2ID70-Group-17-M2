package org.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class question3 {

    public static void solution(SparkSession spark, JavaRDD<String> eventsRDD) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // Map to (seriesId, (timestamp, eventId)) and group by seriesId
        JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> groupedEvents = eventsRDD
                .mapToPair(s -> {
                    String[] parts = s.split(",");
                    return new Tuple2<>(Integer.parseInt(parts[0]),
                            new Tuple2<>(Integer.parseInt(parts[1]), Integer.parseInt(parts[2])));
                })
                .groupByKey();

        // Generate sequences, count, and filter
        JavaRDD<String> sequences = groupedEvents
                .flatMap(new FlatMapFunction<Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>>, String>() {
                    @Override
                    public Iterator<String> call(Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>> series) {
                        List<Tuple2<Integer, Integer>> sortedEvents = new ArrayList<>();
                        series._2.forEach(sortedEvents::add);
                        sortedEvents.sort(java.util.Comparator.comparing(Tuple2::_1));

                        List<String> seqs = new ArrayList<>();
                        for (int i = 0; i <= sortedEvents.size() - 3; i++) {
                            String seq = sortedEvents.get(i)._2 + "," + sortedEvents.get(i + 1)._2 + ","
                                    + sortedEvents.get(i + 2)._2;
                            seqs.add(seq);
                        }
                        return seqs.iterator();
                    }
                })
                .mapToPair(seq -> new Tuple2<>(seq, 1))
                .reduceByKey(Integer::sum)
                .filter(seqCount -> seqCount._2 >= 5)
                .map(Tuple2::_1);

        // Count the distinct sequences
        long q3 = sequences.distinct().count();

        System.out.println(">> [q3: " + q3 + "]");
    }
}
