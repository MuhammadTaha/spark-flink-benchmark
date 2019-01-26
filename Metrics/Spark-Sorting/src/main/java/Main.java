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

        if(args.length == 0){
            System.out.println(" Please run java -jar spark-sorting <path_to_csv_file>");
            return;
        }
        SparkSession spark = SparkSession
                .builder()
                .appName("Main")
                .master("local")
                .getOrCreate();


        Dataset<Row> data = spark.read().option("header","true").schema(schemaBook).csv(args[0]);
        data = data.toDF("id", "userId", "title", "genre","author","pages", "publisher", "date", "price");
        data.orderBy("pages");

        spark.stop();
    }
}
