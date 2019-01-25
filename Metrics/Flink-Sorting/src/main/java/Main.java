import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
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
        // parse parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        // path to ratings.csv file
        String csvPath = params.getRequired("input");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // read a CSV file with three fields
        DataSet<Tuple9<String, String, String, String, String, String, String, String, String>> csvInput = env.readCsvFile(csvPath)
                .ignoreFirstLine()
                .types(String.class, String.class, String.class,
                        String.class, String.class, String.class,
                        String.class, String.class, String.class);

        csvInput.sortPartition(5, Order.ASCENDING).print();
    }
}
