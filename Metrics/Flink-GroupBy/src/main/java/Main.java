import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

public class Main {

    public static void main(String[] args) throws Exception {

        // parse parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        // path to ratings.csv file
        String csvPath = params.getRequired("input");

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> file = env.readTextFile(csvPath);
        file.flatMap(new ExtractInformation()) // after this step is done, the input structure for the groupby and
                                               // sum operator looks like that:
                                               // genre; count
                .groupBy(0) //index 0=genre
                .sum(1) //index 1=number of entries
                .print();
    }

    private static class ExtractInformation implements FlatMapFunction<String, Tuple2<StringValue, Integer>> {
        StringValue genre = new StringValue();
        Tuple2<StringValue, Integer> outputValue = new Tuple2<>(genre, 1);

        @Override
        public void flatMap(String csvLine, Collector<Tuple2<StringValue, Integer>> collector) throws Exception {
            // Every line contains the following values:
            // id, userId, title, genre, author, pages, publisher, date, price
            String[] split = csvLine.split(",");
            String genreFromCSV = split[3];

            if (!genreFromCSV.equals("genre")) { //avoid reading the header
                genre.setValue(genreFromCSV);
                collector.collect(outputValue);
            }
        }
    }
}
