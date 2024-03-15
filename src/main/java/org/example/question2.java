package org.example;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.plans.logical.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

public class question2 {
    public static void solution(SparkSession spark, JavaRDD<String> eventsRDD, JavaRDD<String> eventTypesRDD) {
        StructType eventsSchema = new StructType()
                .add("seriesid", DataTypes.IntegerType)
                .add("timestamp", DataTypes.IntegerType)
                .add("eventid", DataTypes.IntegerType);

        StructType eventTypesSchema = new StructType()
                .add("eventid", DataTypes.IntegerType)
                .add("eventtypeid", DataTypes.IntegerType);

        Dataset<Row> eventsDataset = spark.createDataFrame(eventsRDD.map(row -> {
            String[] parts = row.split(",");
            return RowFactory.create(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
        }), eventsSchema);
        // Might not be consecutive.
        eventsDataset = eventsDataset.withColumn("id", monotonically_increasing_id().cast("Int"));

        Dataset<Row> eventTypesDataset = spark.createDataFrame(eventTypesRDD.map(row -> {
            String[] parts = row.split(",");
            return RowFactory.create(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
        }), eventTypesSchema);
        eventTypesDataset = eventTypesDataset.withColumn("id", monotonically_increasing_id().cast("Int"));

        eventsDataset.createOrReplaceTempView("events");
        Dataset<Row> resultEvents = spark.sql(
            "SELECT COUNT(*) " +
            "FROM events e " +
            "JOIN events p ON e.id = p.id + 1 " +
            "JOIN events n ON e.id = n.id - 1 " + 
            "WHERE p.eventid = 134 AND e.eventid = 8 AND n.eventid = 166"
        );

        //resultEvents.show();
        Row resultRowEvent = resultEvents.first();
        long q21 = resultRowEvent.getAs(0);
        q21 /= 3;

        eventTypesDataset.createOrReplaceTempView("eventtypes");
        Dataset<Row> resultTypes = spark.sql(
            "SELECT COUNT(*) " +
            "FROM eventtypes e " +
            "JOIN eventtypes p ON e.id = p.id + 1 " +
            "JOIN eventtypes n ON e.id = n.id - 1 " + 
            "WHERE p.eventtypeid = 9 AND e.eventtypeid = 6 AND n.eventtypeid = 1"
        );
        eventTypesDataset.show();
        resultTypes.show();
        Row resultRowType = resultTypes.first();
        long q22 = resultRowType.getAs(0);
        
        System.out.println(">> [q21: " + q21 + "]");
        System.out.println(">> [q22: " + q22 + "]");

        //long q21 = 0;

        // List<Row> res = null;
        // res = eventsDataset
        //         .where(eventsDataset.col("seriesid").equalTo("109"))
        //         .where(eventsDataset.col("timestamp").equalTo("145"))
        //         .where(eventsDataset.col("eventid").equalTo("125"))
        //         .collectAsList();
        // System.out.println(res.size());
    }
}
