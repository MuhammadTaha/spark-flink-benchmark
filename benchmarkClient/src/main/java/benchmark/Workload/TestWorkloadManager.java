package benchmark.Workload;

import benchmark.Workload.WorkloadManager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Florian on 15.01.19.
 */
public class TestWorkloadManager {
    public static void main(String[] args) throws InterruptedException {
        String AWS_USERNAME = args[0];
        String AWS_USER_SECRET = args[1];
        String CLUSTER_TYPE = "spark";
        String METRIC_TYPE = "sorting";
        String DATA_SIZE = "20mb"; //1gb, 5gb, 10gb
        int ITER = 1;
        List<Long> executionResults = new ArrayList<Long>();

        //TODO new input parameters:
        // Concrete path to the data file
        //String S3_BUCKET_DATA_FILE = "s3://test-emr-ws1819/data/data.csv";
        String S3_BUCKET_DATA_FILE = "s3://ws1819-as3-group15-data2/csv/data_20m.csv";
        //log directory for EMR cluster; just an empty directory in a S3 bucket
        String EMR_LOG_DIR = "s3://ws1819-as3-group15-data2/csv/";
        // ####### End new parameters


        //How to call the WorkloadManager
        WorkloadManager workloadManager = new WorkloadManager(
                AWS_USERNAME,
                AWS_USER_SECRET,
                EMR_LOG_DIR);
        executionResults = workloadManager.startBenchmark(
                CLUSTER_TYPE,
                METRIC_TYPE,
                S3_BUCKET_DATA_FILE,
                AWS_USERNAME,
                AWS_USER_SECRET,
                EMR_LOG_DIR,
                ITER);

        System.out.println("Results:");
        try{
            long dataSizeInMb = 0;
            double executionTimeInSec = 0;
            if (DATA_SIZE.contains("mb")){
                dataSizeInMb = Long.parseLong((DATA_SIZE.replaceAll("mb","")));
            } else if (DATA_SIZE.contains("gb")){
                dataSizeInMb = Long.parseLong((DATA_SIZE.replaceAll("gb",""))) * Long.parseLong("1024");
            }

            String file_name = CLUSTER_TYPE+"_"+METRIC_TYPE+"_"+DATA_SIZE+"_results.csv";
            File file = new File(file_name);
            FileWriter fw = new FileWriter(file);
            BufferedWriter bw = new BufferedWriter(fw);

            bw.write("ClusterType,Metric,Runtime,Throughput");
            bw.newLine();
            for(int i=0;i<executionResults.size();i++)
            {
                executionTimeInSec = (double) Long.parseLong(""+ executionResults.get(i)) / 1000;
                double throughput = (double)dataSizeInMb / (double)executionTimeInSec;
                bw.write(CLUSTER_TYPE+","+METRIC_TYPE+","+executionResults.get(i)+","+throughput);
                bw.newLine();
            }

            bw.close();
            fw.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}
