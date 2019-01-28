import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;

public class Main {

    public static void main(String[] args) throws Exception {
        // parse parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        // path to ratings.csv file
        String csvPath = params.getRequired("input");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // read the CSV file
        DataSet<Tuple9<String, String, String, String, String, String, String, String, String>> csvInput = env.readCsvFile(csvPath)
                .ignoreFirstLine()
                .types(String.class, String.class, String.class,
                        String.class, String.class, String.class,
                        String.class, String.class, String.class);

        csvInput.max(8).first(1).print();
    }
}
