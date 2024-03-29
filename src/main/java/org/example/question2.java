package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.monotonically_increasing_id;

import javax.xml.crypto.Data;

public class question2 {
    public static void solution(SparkSession spark, JavaRDD<String> eventsRDD, JavaRDD<String> eventTypesRDD) {
        
        // Schema for event dataframe
        StructType eventsSchema = new StructType()
                .add("seriesid", DataTypes.IntegerType)
                .add("timestamp", DataTypes.IntegerType)
                .add("eventid", DataTypes.IntegerType);

        // We create a dataframe from the RDD by mapping every line to a Row object
        Dataset<Row> eventsDataset = spark.createDataFrame(eventsRDD.map(row -> {
            String[] parts = row.split(",");
            return RowFactory.create(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
        }), eventsSchema);

        // Sort on series and timestamp in order to get sequences
        //eventsDataset = eventsDataset.sort("seriesid", "timestamp");

        // Add a unique id for joining the tables
        //eventsDataset = eventsDataset.withColumn("id", monotonically_increasing_id());
        
        // Schema for eventtypes
        StructType eventTypesSchema = new StructType()
        .add("eventid", DataTypes.IntegerType)
        .add("eventtypeid", DataTypes.IntegerType);

        // We create a dataframe from the RDD by mapping every line to a Row object
        Dataset<Row> eventTypesDataset = spark.createDataFrame(eventTypesRDD.map(row -> {
            String[] parts = row.split(",");
            return RowFactory.create(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
        }), eventTypesSchema);

        // Query for 2.1
        eventsDataset.createOrReplaceTempView("events");
        // Dataset<Row> resultEvents = spark.sql(
        //     "SELECT COUNT(*) " +
        //     "FROM events e " +
        //     "JOIN events p ON e.id = (p.id + 1) AND e.seriesid = p.seriesid " +
        //     "JOIN events n ON e.id = (n.id - 1) AND e.seriesid = p.seriesid " + 
        //     "WHERE p.eventid = 109 AND e.eventid = 145 AND n.eventid = 125"
        // );
        Dataset<Row> resultEvents = spark.sql(
            "SELECT COUNT(*) as total_occurences " + 
            "FROM events AS e1, events AS e2, events AS e3 " +
            "WHERE e2.timestamp = (e1.timestamp + 1) AND e1.seriesid = e2.seriesid " + 
            "AND e2.timestamp = (e3.timestamp - 1) AND e2.seriesid = e3.seriesid " + 
            "AND e1.eventid = 109 AND e2.eventid = 145 AND e3.eventid = 125" 
        );

        Row resultRowEvent = resultEvents.first();
        long q21 = resultRowEvent.getAs(0);

        // Query for 2.2, same as 2.1 except we map first
        eventTypesDataset.createOrReplaceTempView("eventtypes");
        // Dataset<Row> resultTypes = spark.sql(
        //     "WITH mappedEvents(seriesid, timestamp, eventtypeid, id) AS (SELECT e.seriesid, e.timestamp, t.eventtypeid, e.id " +
        //     "FROM events e JOIN eventtypes t ON t.eventid = e.eventid) " +

        //     "SELECT COUNT(*) " +
        //     "FROM mappedEvents e " +
        //     "JOIN mappedEvents p ON e.id = (p.id + 1) AND e.seriesid = p.seriesid " +
        //     "JOIN mappedEvents n ON e.id = (n.id - 1) AND e.seriesid = p.seriesid " +
        //     "WHERE p.eventtypeid = 2 AND e.eventtypeid = 11 AND n.eventtypeid = 6"
        // );

        Dataset<Row> resultTypes = spark.sql(
            "WITH mappedEvents(seriesid, timestamp, eventtypeid) AS (SELECT e.seriesid, e.timestamp, t.eventtypeid " +
            "FROM events e JOIN eventtypes t ON t.eventid = e.eventid) " +

            "SELECT COUNT(*) as total_occurences " + 
            "FROM mappedEvents AS e1, mappedEvents AS e2, mappedEvents AS e3 " +
            "WHERE e2.timestamp = (e1.timestamp + 1) AND e1.seriesid = e2.seriesid " + 
            "AND e2.timestamp = (e3.timestamp - 1) AND e2.seriesid = e3.seriesid " + 
            "AND e1.eventtypeid = 2 AND e2.eventtypeid = 11 AND e3.eventtypeid = 6" 
        );

        Row resultRowType = resultTypes.first();
        long q22 = resultRowType.getAs(0);
        
        System.out.println(">> [q21: " + q21 + "]");
        System.out.println(">> [q22: " + q22 + "]");
    }
}
