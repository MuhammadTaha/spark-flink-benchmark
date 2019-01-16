package benchmark.Workload;

import benchmark.Workload.WorkloadManager;

/**
 * Created by Florian on 15.01.19.
 */
public class TestWorkloadManager {
    public static void main(String[] args) {
        String AWS_USERNAME = args[0];
        String AWS_USER_SECRET = args[1];
        String CLUSTER_TYPE = "spark";
        String METRIC_TYPE = "groupby";


        //TODO new input parameters:
        // Concrete path to the data file
        String S3_BUCKET_DATA_FILE = "s3://test-emr-ws1819/data/test.txt";
        //log directory for EMR cluster; just an empty directory in a S3 bucket
        String EMR_LOG_DIR = "s3://test-emr-ws1819/log/";
        // ####### End new parameters


        //How to call the WorkloadManager
        WorkloadManager workloadManager = new WorkloadManager(
                AWS_USERNAME,
                AWS_USER_SECRET,
                EMR_LOG_DIR);
        workloadManager.startBenchmark(
                CLUSTER_TYPE,
                METRIC_TYPE,
                S3_BUCKET_DATA_FILE,
                AWS_USERNAME,
                AWS_USER_SECRET,
                EMR_LOG_DIR);
    }
}
