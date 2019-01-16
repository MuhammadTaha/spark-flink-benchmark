package benchmark.Workload;

/**
 * Created by Florian on 15.01.19.
 */
public interface IClusterManager {
    static final String PATH_TO_JAR_FILES = "s3://ws1819-as3-group15/JARFiles";
    static final String PATH_TO_BOOTSTRAP_SCRIPT  = "s3://ws1819-as3-group15/Bootstrap/bootstrap-actions.sh";

    //Metrics
    static final String METRIC_FLINK_AGG = "AggregationFlink.jar";
    static final String METRIC_FLINK_GRB = "GroupByFlink.jar";
    static final String METRIC_FLINK_SOR = "SortingFlink.jar";
    static final String METRIC_SPARK_AGG = "AggregationSpark.jar";
    static final String METRIC_SPARK_GRB = "GroupBySpark.jar";
    static final String METRIC_SPARK_SOR = "SortingSpark.jar";

    /**
     *
     * @return execution time in ms
     */
    long startBenchmark();
}
