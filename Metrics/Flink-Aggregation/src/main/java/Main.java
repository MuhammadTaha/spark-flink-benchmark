import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;
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
        file.flatMap(new ExtractInformation()).max(0).print();
    }

    private static class ExtractInformation implements FlatMapFunction<String, Tuple2<DoubleValue, Integer>> {
        DoubleValue price = new DoubleValue();
        Tuple2<DoubleValue, Integer> outputValue = new Tuple2<>(price, 1);

        @Override
        public void flatMap(String csvLine, Collector<Tuple2<DoubleValue, Integer>> collector) throws Exception {
            // Every line contains the following values:
            // id, userId, title, genre, author, pages, publisher, date, price
            String[] split = csvLine.split(",");
            String priceFromCSV = split[8];

            if (!priceFromCSV.equals("price")) { //avoid reading the header
                try {
                    price.setValue(Double.parseDouble(priceFromCSV));
                    collector.collect(outputValue);
                } catch (Exception e) {
                    //Do nothing
                }
            }
        }
    }
}
