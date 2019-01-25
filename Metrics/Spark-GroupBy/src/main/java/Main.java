import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;



public class Main {

    public static void main(String[] args) throws Exception {

        if(args.length == 0){
            System.out.println(" Please run java -jar spark-groupby <path_to_csv_file>");
            return;
        }

        SparkConf conf = new  SparkConf().setMaster("local").setAppName("Main");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        Dataset<Row> data = spark.read().csv(args[0]);
        data = data.toDF("id", "userId", "title", "genre", "author", "pages", "publisher", "date", "price");
        Dataset<Row> genre = data.groupBy("genre").count();
        genre.show(data.collectAsList().size(),false);


        spark.stop();
    }
}
