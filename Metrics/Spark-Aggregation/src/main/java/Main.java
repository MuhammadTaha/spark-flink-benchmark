import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.max;

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
                DataTypes.createStructField("price", DataTypes.FloatType, true),

        });

        if(args.length == 0){
            System.out.println(" Please run java -jar spark-aggregation <path_to_csv_file>");
            return;
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("Main")
                .master("local")
                .getOrCreate();


        Dataset<Row> data = spark.read().option("header","true").schema(schemaBook).csv(args[0]);
        data = data.toDF("id", "userId", "title", "genre", "author", "pages", "publisher", "date", "price");
        data.agg(max("price")).show();

        // Uncomment the follwoing lines to save the result into a csv file
        // Dataset<Row> max_price = data.agg(max("price"));
        // max_price.coalesce(1).write().format("com.databricks.spark.csv").option("header", "false").save("aggregate.csv");

        spark.stop();
    }


}
