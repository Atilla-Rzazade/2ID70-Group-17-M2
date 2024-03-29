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
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

public class question3_boris {

    public static void solution(SparkSession spark, JavaRDD<String> eventsRDD) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaPairRDD<Tuple2<Integer, Integer>, ArrayList<Integer>> events = eventsRDD
            .flatMapToPair(line -> {
                String[] parts = line.split(",", 3);
                
                List<Tuple2<Tuple2<Integer, Integer>, ArrayList<Integer>>> result = new ArrayList<>();
                for (int i=0;i<3;i++) {
                    Integer timestamp = Integer.parseInt(parts[1]) + i;
                    Tuple2<Integer, Integer> key = new Tuple2<>(Integer.parseInt(parts[0]), timestamp);

                    ArrayList<Integer> list = new ArrayList<Integer>(Arrays.asList(Integer.parseInt(parts[2])));
                    result.add(new Tuple2<>(key, list));
                }
                
                return result.iterator();
            });

        JavaPairRDD<Tuple2<Integer, Integer>, ArrayList<Integer>> reducedEvents = events
            .reduceByKey((list1, list2) -> {
                list1.addAll(list2);
                return list1;
            });

        JavaPairRDD<Tuple2<Integer, Integer>, ArrayList<Integer>> filteredEvents = reducedEvents
            .filter(line -> line._2().size() == 3);

        JavaPairRDD<Tuple2<Integer, ArrayList<Integer>>, Integer> sequenceCounts = filteredEvents
            .mapToPair(line -> {
                Integer seriesId = line._1._1;
                Tuple2<Integer, ArrayList<Integer>> key = new Tuple2<>(seriesId, line._2);
                return new Tuple2<>(key, 1);
            })
            .reduceByKey((count1, count2) -> count1 + count2);

        JavaRDD<ArrayList<Integer>> RDDQ3 = sequenceCounts
            .filter(sequence -> sequence._2() >= 5)
            .map(sequence -> sequence._1._2);

        // List<Tuple2<String, ArrayList<String>>> temp = RDDQ3.collect();
        // for(int i=0;i<50;i++) {
        //     System.out.println(temp.get(i));
        // }

        long q3 = RDDQ3.distinct().count();

        System.out.println(">> [q3: " + q3 + "]");
        
    }
}
