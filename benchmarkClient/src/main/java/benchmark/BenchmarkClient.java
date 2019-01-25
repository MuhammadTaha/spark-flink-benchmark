package benchmark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import benchmark.Workload.WorkloadManager;

public class BenchmarkClient {

	static String AWS_USERNAME = "";
	static String AWS_USER_SECRET = "";
	static String CLUSTER_TYPE = "";
	static String METRIC_TYPE = "";
	static String S3_BUCKET_DATA_FILE = "";
	static String EMR_LOG_DIR = "";
	static String DATA_SIZE = "";
	static int NUMBER_OF_ITERATIONS = 1;

	public static void main(String[] args) throws InterruptedException {

		if (!ArgumentValidator.validArguments(args)) {
			return;
		}

		AWS_USERNAME = args[0];
		AWS_USER_SECRET = args[1];
		CLUSTER_TYPE = args[2];
		METRIC_TYPE = args[3];
		S3_BUCKET_DATA_FILE = args[4];
		EMR_LOG_DIR = args[5];
		DATA_SIZE = args.length >= 7 ? args[6] : DATA_SIZE; //e.g 4mb or 1 gb
		NUMBER_OF_ITERATIONS = args.length == 8 ? Integer.parseInt(args[7]): NUMBER_OF_ITERATIONS; // default of iterations is 1

		System.out.println("Arguments:" + "\nAWS_USERNAME = " + AWS_USERNAME + "\nAWS_USER_SECRET = " + AWS_USER_SECRET
				+ "\nCLUSTER_TYPE = " + CLUSTER_TYPE + "\nMETRIC_TYPE = " + METRIC_TYPE + "\nS3_BUCKET_DATA_FILE = "
				+ S3_BUCKET_DATA_FILE + "\nEMR_LOG_DIR = " + EMR_LOG_DIR + "\nDATA_SIZE = " + DATA_SIZE);

		/*// TODO - Generate Fake Data
		DataGenerator dataGenerator = new DataGenerator();
		File dataFile = dataGenerator.generateData(DATA_SIZE);

		// TODO upload
		if (!S3_BUCKET_DATA_FILE.isEmpty() && !DATA_SIZE.isEmpty()) {
			S3Connector s3 = new S3Connector(AWS_USERNAME, AWS_USER_SECRET, S3_BUCKET_DATA_FILE, "region",
					S3_BUCKET_DATA_FILE, "bucketName");
			s3.upload(dataFile);
		}*/

		// TODO - WorkloadManager
		List<Long> executionResults = new ArrayList<Long>();
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
				NUMBER_OF_ITERATIONS);

		// Write results into csv file
		try{
			long dataSizeInMb = 0;
			double executionTimeInSec = 0;
			if (DATA_SIZE.contains("mb")){
				dataSizeInMb = Long.parseLong((DATA_SIZE.replaceAll("mb","")));
			} else if (DATA_SIZE.contains("gb")){
				dataSizeInMb = Long.parseLong((DATA_SIZE.replaceAll("gb",""))) * Long.parseLong("1024");
			}

			String file_name = CLUSTER_TYPE+"_"+METRIC_TYPE+"_results.csv";
			File file = new File(file_name);
			FileWriter fw = new FileWriter(file);
			BufferedWriter bw = new BufferedWriter(fw);

			bw.write("ClusterType,Metric,Runtime,Throughput");
			bw.newLine();
			for(int i=0;i<executionResults.size();i++)
			{
				executionTimeInSec = (double) Long.parseLong(""+ executionResults.get(i)) / 1000;
				bw.write(CLUSTER_TYPE+","+METRIC_TYPE+","+executionResults.get(i)+","+executionResults.get(i)+(double)dataSizeInMb / (double)executionTimeInSec);
				bw.newLine();
			}

			bw.close();
			fw.close();
		} catch (IOException e) {
			System.out.println(e);
		}

	}
}
