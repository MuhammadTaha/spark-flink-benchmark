package benchmark.Workload;

/**
 * This class is used for managing the workloads on the AWS EMR clusters
 */
public class WorkloadManager {

    private String AWS_USERNAME;
    private String AWS_USER_SECRET;
    private String EMR_LOG_DIR;

    /**
     * @param AWS_USERNAME AWS Credentials
     * @param AWS_USER_SECRET AWS Credentials
     * @param EMR_LOG_DIR Log directory for the EMR cluster (it's just an empty directory in your S3 bucket)
     */
    public  WorkloadManager(String AWS_USERNAME, String AWS_USER_SECRET,
                            String EMR_LOG_DIR) {
            this.AWS_USERNAME = AWS_USERNAME;
            this.AWS_USER_SECRET = AWS_USER_SECRET;
            this.EMR_LOG_DIR = EMR_LOG_DIR;

    }

    /**
     * Start an EMR cluster, submit the job to the cluster
     * and wait until the execution is done.
     *
     * @param CLUSTER_TYPE
     * @param METRIC_TYPE
     * @param S3_BUCKET_DATA_FILE Path to the input file
     * @return Execution time in ms
     */
    public long startBenchmark(String CLUSTER_TYPE, String METRIC_TYPE, String S3_BUCKET_DATA_FILE,
                               String AWSKey, String AWSSecret, String EMRLogDir){

        ClusterManager clusterManager = new ClusterManager(METRIC_TYPE,
                S3_BUCKET_DATA_FILE,
                AWSKey,
                AWSSecret,
                EMRLogDir,
                CLUSTER_TYPE);
        return clusterManager.startBenchmark();


    }
}
