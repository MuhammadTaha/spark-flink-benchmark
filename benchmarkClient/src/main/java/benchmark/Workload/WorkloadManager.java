package benchmark.Workload;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used for managing the workloads on the AWS EMR clusters
 */
public class WorkloadManager {

    private String AWS_USERNAME;
    private String AWS_USER_SECRET;
    private String EMR_LOG_DIR;
    List<Long> executionResults = new ArrayList<Long>();

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
     * @return Execution time in ms (List)
     */
    public List<Long> startBenchmark(String CLUSTER_TYPE, String METRIC_TYPE, String S3_BUCKET_DATA_FILE,
                                        String AWSKey, String AWSSecret, String EMRLogDir, int numberOfIteration) throws InterruptedException {


        //execute EMR clusters in parallel
        Thread emrThreads[] = new Thread[1];
        for (int i = 0; i < emrThreads.length; i++)
        {
            Thread T1 = new Thread(new EMRThread(CLUSTER_TYPE, METRIC_TYPE, S3_BUCKET_DATA_FILE,
                    AWSKey, AWSSecret, EMRLogDir, numberOfIteration));
            T1.start();
            emrThreads[i] = T1;
        }

        //wait until each cluster execution is done
        for (int i = 0; i < emrThreads.length; i++)
        {
            emrThreads[i].join();
        }

        return executionResults;


    }

    private class EMRThread implements Runnable {
        String CLUSTER_TYPE;
        String METRIC_TYPE;
        String S3_BUCKET_DATA_FILE;
        String AWSKey;
        String AWSSecret;
        String EMRLogDir;
		private int numberOfIteration;

        public EMRThread(String CLUSTER_TYPE, String METRIC_TYPE, String S3_BUCKET_DATA_FILE,
                         String AWSKey, String AWSSecret, String EMRLogDir, int numberOfIteration){
            this.CLUSTER_TYPE = CLUSTER_TYPE;
            this.METRIC_TYPE = METRIC_TYPE;
            this.S3_BUCKET_DATA_FILE = S3_BUCKET_DATA_FILE;
            this.AWSKey = AWSKey;
            this.AWSSecret = AWSSecret;
            this.EMRLogDir = EMRLogDir;
            this.numberOfIteration = numberOfIteration;
        }

        @Override
        public void run() {
            ClusterManager clusterManager = new ClusterManager(METRIC_TYPE,
                    S3_BUCKET_DATA_FILE,
                    AWSKey,
                    AWSSecret,
                    EMRLogDir,
                    CLUSTER_TYPE, numberOfIteration);

            executionResults.add(clusterManager.startBenchmark());
        }
    }
}
