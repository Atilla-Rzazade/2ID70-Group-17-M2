package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.monotonically_increasing_id;

public class question2 {
    public static void solution(SparkSession spark, JavaRDD<String> eventsRDD, JavaRDD<String> eventTypesRDD) {
        StructType eventsSchema = new StructType()
                .add("seriesid", DataTypes.IntegerType)
                .add("timestamp", DataTypes.IntegerType)
                .add("eventid", DataTypes.IntegerType);

        Dataset<Row> eventsDataset = spark.createDataFrame(eventsRDD.map(row -> {
            String[] parts = row.split(",");
            return RowFactory.create(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
        }), eventsSchema);
        eventsDataset = eventsDataset.sort("seriesid", "timestamp");
        eventsDataset = eventsDataset.withColumn("id", monotonically_increasing_id());

        eventsDataset.show();
        
        StructType eventTypesSchema = new StructType()
        .add("eventid", DataTypes.IntegerType)
        .add("eventtypeid", DataTypes.IntegerType);

        Dataset<Row> eventTypesDataset = spark.createDataFrame(eventTypesRDD.map(row -> {
            String[] parts = row.split(",");
            return RowFactory.create(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
        }), eventTypesSchema);

        eventsDataset.createOrReplaceTempView("events");
        Dataset<Row> resultEvents = spark.sql(
            "SELECT COUNT(*) " +
            "FROM events e " +
            "JOIN events p ON e.id = (p.id + 1) " +
            "JOIN events n ON e.id = (n.id - 1) " + 
            "WHERE p.eventid = 109 AND e.eventid = 145 AND n.eventid = 125"
        );

        resultEvents.show();
        Row resultRowEvent = resultEvents.first();
        long q21 = resultRowEvent.getAs(0);

        eventTypesDataset.createOrReplaceTempView("eventtypes");
        Dataset<Row> resultTypes = spark.sql(
            "SELECT COUNT(*) " +
            "FROM events e " +
            "JOIN events p ON e.id = (p.id + 1) " +
            "JOIN events n ON e.id = (n.id - 1) " + 
            "JOIN eventtypes t ON e.eventid = t.eventid " +
            "WHERE p.eventtypeid = 2 AND e.eventtypeid = 11 AND n.eventtypeid = 6"
        );
        
        //resultTypes.show();
        Row resultRowType = resultTypes.first();
        long q22 = resultRowType.getAs(0);
        
        System.out.println(">> [q21: " + q21 + "]");
        System.out.println(">> [q22: " + q22 + "]");

        // List<Row> res = null;
        // res = eventsDataset
        //         .where(eventsDataset.col("seriesid").equalTo("109"))
        //         .where(eventsDataset.col("timestamp").equalTo("145"))
        //         .where(eventsDataset.col("eventid").equalTo("125"))
        //         .collectAsList();
        // System.out.println(res.size());
    }
}
