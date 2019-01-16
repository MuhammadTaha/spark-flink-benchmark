package benchmark;


import benchmark.Workload.WorkloadManager;

public class BenchmarkClient {

    static String AWS_USERNAME = "";
    static String AWS_USER_SECRET = "";
    static String CLUSTER_TYPE = "";
    static String METRIC_TYPE = "";
    static String S3_BUCKET_DATA_FILE = "";
    static String EMR_LOG_DIR = "";
    static int DATA_SIZE = 1;

    public static void main(String[] args) {

        if(!ArgumentValidator.validArguments(args)) {
            return;
        }

        AWS_USERNAME = args[0];
        AWS_USER_SECRET = args[1];
        CLUSTER_TYPE = args[2];
        METRIC_TYPE = args[3];
        S3_BUCKET_DATA_FILE = args[4];
        EMR_LOG_DIR = args[5];
        DATA_SIZE = args.length == 7 ?  Integer.parseInt(args[6]) : DATA_SIZE ;

        System.out.println("Arguments:"
                +"\nAWS_USERNAME = " + AWS_USERNAME
                +"\nAWS_USER_SECRET = " + AWS_USER_SECRET
                +"\nCLUSTER_TYPE = " + CLUSTER_TYPE
                + "\nMETRIC_TYPE = "+ METRIC_TYPE
                + "\nS3_BUCKET_DATA_FILE = "+ S3_BUCKET_DATA_FILE
                + "\nEMR_LOG_DIR = "+ EMR_LOG_DIR
                + "\nDATA_SIZE = "+ DATA_SIZE);

        //TODO - Generate Fake Data
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.generateData(DATA_SIZE);

        // TODO - WorkloadManager
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


        // TODO - MetricsManager -> is this really necessary ?

        // TODO - Plot CSV results

    }
}
