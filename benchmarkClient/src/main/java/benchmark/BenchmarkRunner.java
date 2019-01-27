package benchmark;

import java.util.Arrays;

public class BenchmarkRunner {
	static String AWS_USERNAME = "";
	static String AWS_USER_SECRET = "";
	static String[] CLUSTER_TYPEs = { "spark", "flink" };
	static String[] METRIC_TYPE = { "sorting", "aggregate", "groupby"};
	static String S3_BUCKET_DATA_FILEs = "s3://ws1819-as3-group15-data2/csv/data_%sg.csv";
	static String EMR_LOG_DIR = "s3://ws1819-as3-group15-data2/csv/";
	static int[] DATA_SIZES = { 1, 5, 10 };
	static int NUMBER_OF_ITERATIONS = 100;
	static String AWS_SESSION_TOKEN = "";

	public static void main(String[] args) {
		for (int dataSize : DATA_SIZES) {
			for (String clusterType : CLUSTER_TYPEs) {
				for (String metricType : METRIC_TYPE) {
					BenchmarkClient bc = new BenchmarkClient();
					String[] generatedArgs = new String[] { AWS_USERNAME, AWS_USER_SECRET, clusterType, metricType,
							String.format(S3_BUCKET_DATA_FILEs, dataSize), EMR_LOG_DIR, dataSize + "gb",
							Integer.toString(NUMBER_OF_ITERATIONS)};
					System.out.println("Running Benchmark with arguments: " + Arrays.toString(generatedArgs));
					try {
						bc.main(generatedArgs);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

}
