import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.joda.time.DateTime;

public class Main {

    public static void main(String[] args) throws Exception {

        // for batch programs use ExecutionEnvironment instead of StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create a TableEnvironment
        // for batch programs use BatchTableEnvironment instead of StreamTableEnvironment
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
    }

    static class TupleConverter implements MapFunction<String, Tuple9<Integer, Integer, String, String, String, Integer, String, DateTime, Double>> {

        @Override
        public Tuple9<Integer, Integer, String, String, String, Integer, String, DateTime, Double> map(String csvLine) throws Exception {
            String[] split = csvLine.split(",");

            if (!split[0].equals("id")){
                return Tuple9.of(Integer.parseInt(split[0]),
                        Integer.parseInt(split[1]),
                        split[2],
                        split[3],
                        split[4],
                        Integer.parseInt(split[5]),
                        split[6],
                        DateTime.parse(split[7]),
                        Double.parseDouble(split[8]));
            }

        }
    }
}
