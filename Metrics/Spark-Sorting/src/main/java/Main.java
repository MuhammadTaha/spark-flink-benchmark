import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.sql.Timestamp;

import java.time.Duration;
import java.time.Instant;

import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;

import org.apache.log4j.PropertyConfigurator;


public class Main {


    private static final SimpleDateFormat sdf = new SimpleDateFormat("mm.ss.SS");

    static Logger logger = Logger.getLogger(Main.class);
    static Logger fileLogger = Logger.getLogger("FILE-LOG");


    public static void main(String[] args) throws Exception {


         StructType schemaBook = DataTypes.createStructType(new StructField[] {

                DataTypes.createStructField("id",  DataTypes.IntegerType, true),
                DataTypes.createStructField("userId", DataTypes.StringType, true),
                DataTypes.createStructField("title", DataTypes.StringType, true),
                DataTypes.createStructField("genre", DataTypes.StringType, true),
                DataTypes.createStructField("author", DataTypes.StringType, true),
                DataTypes.createStructField("pages", DataTypes.IntegerType, true),
                DataTypes.createStructField("publisher", DataTypes.StringType, true),
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("price", DataTypes.StringType, true),

        });


        PropertyConfigurator.configure("log4j.properties");

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());


        Instant start = Instant.now();

        fileLogger.trace("Starting Spark sorting : "+timestamp);


//        Logger.getLogger("Sorting time").info("here taha");//sdf.format(timestamp));

        if(args.length == 0){
            System.out.println(" Please run java -jar spark-sorting <path_to_csv_file>");
            return;
        }
        SparkSession spark = SparkSession
                .builder()
                .appName("Main")
                .master("local")
                .getOrCreate();


//      Dataset<Row> data = spark.read().csv(args[0]);

        Dataset<Row> data = spark.read().option("header","true").schema(schemaBook).csv(args[0]);

        data.printSchema();

//        System.out.println(data);
        data = data.toDF("id", "userId", "title", "genre","author","pages", "publisher", "date", "price");


        Dataset<Row> fnameDS = data.orderBy("pages");

        // for writing one huge csv file
//        fnameDS.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save("output.csv");

        fnameDS.collect();
        fnameDS.persist();
//
        fnameDS.show(168115,false);



        // for reading csv
//        Dataset<Row> df = spark.read().format("com.databricks.spark.csv").option("header", "true").load("output.csv");
//        df.show(df.collectAsList().size(),false);
//        df.toDF().show(false);


        Instant end = Instant.now();

        Duration timeElapsed = Duration.between(start, end);

        System.out.println("Time taken: "+ timeElapsed.toMillis() +" milliseconds");

        fileLogger.trace(" Spark sorting delay : "+ timeElapsed.toMillis() );

        spark.stop();
    }
}
