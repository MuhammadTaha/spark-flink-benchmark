package benchmark;

import java.io.File;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class BenchmarkClient {

	static String AWS_USERNAME = "";
	static String AWS_USER_SECRET = "";
	static String CLUSTER_TYPE = "";
	static String METRIC_TYPE = "";
	static String S3_BUCKET_DATA_FILE = "";
	static String EMR_LOG_DIR = "";
	static String DATA_SIZE = "";
	static int NUMBER_OF_ITERATIONS = 1;
	static String AWS_SESSION_TOKEN = "";

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
		DATA_SIZE = args.length >= 7 ? args[6] : DATA_SIZE; // e.g 4mb or 1 gb
		NUMBER_OF_ITERATIONS = args.length >= 8 ? Integer.parseInt(args[7]) : NUMBER_OF_ITERATIONS; // default of
																									// iterations is 1
		AWS_SESSION_TOKEN = args.length >= 9 ? args[8] : "";

		System.out.println("Arguments:" + "\nAWS_USERNAME = " + AWS_USERNAME + "\nAWS_USER_SECRET = " + AWS_USER_SECRET
				+ "\nCLUSTER_TYPE = " + CLUSTER_TYPE + "\nMETRIC_TYPE = " + METRIC_TYPE + "\nS3_BUCKET_DATA_FILE = "
				+ S3_BUCKET_DATA_FILE + "\nEMR_LOG_DIR = " + EMR_LOG_DIR + "\nDATA_SIZE = " + DATA_SIZE
				+ "\nAWS_SESSION_TOKEN = " + AWS_SESSION_TOKEN);

		// generation and upload
		AmazonS3URI uri = new AmazonS3URI(S3_BUCKET_DATA_FILE);
		AmazonS3 s3;
		s3 = AmazonS3ClientBuilder.standard().withRegion("us-east-1").withCredentials(new AWSStaticCredentialsProvider(
				new BasicSessionCredentials(AWS_USERNAME, AWS_USER_SECRET, AWS_SESSION_TOKEN))).build();
		if (!s3.doesObjectExist(uri.getBucket(), uri.getKey())) {
			if (!DATA_SIZE.isEmpty()) {
				DataGenerator dataGenerator = new DataGenerator();
				File dataFile = dataGenerator.generateData(DATA_SIZE);
				System.out.print("Uploading...");
				PutObjectRequest request = new PutObjectRequest(uri.getBucket(), uri.getKey(), dataFile);
				ObjectMetadata metadata = new ObjectMetadata();
				metadata.setContentType("plain/text");
				request.setMetadata(metadata);
				s3.putObject(request);
				System.out.println(" done! (" + uri.toString() + ")");
			} else {
				System.err.println("Datafile does not exist in bucket and no generation parameter set. Exitting here.");
				return;
			}
		} else {
			System.out.println("file [" + uri.getURI().toString() + "] exists");
		}

		// TODO - WorkloadManager
//		List<Long> executionResults = new ArrayList<Long>();
//		WorkloadManager workloadManager = new WorkloadManager(AWS_USERNAME, AWS_USER_SECRET, EMR_LOG_DIR);
//		executionResults = workloadManager.startBenchmark(CLUSTER_TYPE, METRIC_TYPE, S3_BUCKET_DATA_FILE, AWS_USERNAME,
//				AWS_USER_SECRET, EMR_LOG_DIR, NUMBER_OF_ITERATIONS);
//
//		// Write results into csv file
//		try {
//			long dataSizeInMb = 0;
//			double executionTimeInSec = 0;
//			if (DATA_SIZE.contains("mb")) {
//				dataSizeInMb = Long.parseLong((DATA_SIZE.replaceAll("mb", "")));
//			} else if (DATA_SIZE.contains("gb")) {
//				dataSizeInMb = Long.parseLong((DATA_SIZE.replaceAll("gb", ""))) * Long.parseLong("1024");
//			}
//
//			String file_name = CLUSTER_TYPE + "_" + METRIC_TYPE + "_results.csv";
//			File file = new File(file_name);
//			FileWriter fw = new FileWriter(file);
//			BufferedWriter bw = new BufferedWriter(fw);
//
//			bw.write("ClusterType,Metric,Runtime,Throughput");
//			bw.newLine();
//			for (int i = 0; i < executionResults.size(); i++) {
//				executionTimeInSec = (double) Long.parseLong("" + executionResults.get(i)) / 1000;
//				bw.write(CLUSTER_TYPE + "," + METRIC_TYPE + "," + executionResults.get(i) + ","
//						+ executionResults.get(i) + dataSizeInMb / executionTimeInSec);
//				bw.newLine();
//			}
//
//			bw.close();
//			fw.close();
//		} catch (IOException e) {
//			System.out.println(e);
//		}
//
	}
}
