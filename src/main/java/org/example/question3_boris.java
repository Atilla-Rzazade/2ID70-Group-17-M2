package org.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.Comparator;

public class question3_boris {
    
    public static void solution(SparkSession spark, JavaRDD<String> eventsRDD) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaPairRDD<String, Iterable<Tuple2<Integer, String>>> groupedEvents = eventsRDD
                .mapToPair(s -> {
                    String[] parts = s.split(",");
                    return new Tuple2<>(parts[0],
                            new Tuple2<>(Integer.parseInt(parts[1]), parts[2]));
                })
                .groupByKey();

        JavaRDD<String> sequences = groupedEvents
                .flatMap(tuple -> {
                    List<Tuple2<Integer, String>> list = new ArrayList<>();
                    tuple._2.forEach(list::add);
                    list.sort(Comparator.comparing(Tuple2::_1));
                    
                    List<String> seqs = new ArrayList<>();
                    for (int i = 0; i < list.size() - 2; i++) {
                        String seq = tuple._1 + "," + list.get(i)._2 + "," + list.get(i + 1)._2 + ","
                                + list.get(i + 2)._2;
                        seqs.add(seq);
                    }
                    return seqs.iterator();
                });

        JavaRDD<String> RDDQ3 = sequences
                .mapToPair(seq -> new Tuple2<>(seq, 1))
                .reduceByKey(Integer::sum)
                .filter(seqCount -> seqCount._2 >= 5)
                .map(Tuple2::_1);

        Set<String> eventSequences = new HashSet<>(RDDQ3.collect());
        System.out.println(eventSequences.size());
        List<String> cleanSequences = new ArrayList<>(eventSequences);
        System.out.println(cleanSequences.size());
        
        long q3 = RDDQ3.distinct().count();

        System.out.println(">> [q3: " + q3 + "]");
    }
}
