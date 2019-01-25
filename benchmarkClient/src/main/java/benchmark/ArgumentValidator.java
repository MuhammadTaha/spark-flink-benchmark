package benchmark;

import java.util.Arrays;

public class ArgumentValidator {

	private static String[] METRIC_TYPES = { "groupby", "sorting", "aggregate" };
	private static String[] CLUSTER_TYPES = { "flink", "spark" };

	public static boolean validArguments(String[] inputArgs) {

		if (inputArgs.length < 5 || inputArgs.length > 8) {
			System.out.println(
					"Please run: java -jar benchmarkclient-1.0.jar <user> <secret> <cluster_type> <metric_type> <s3_bucket_data_file> <s3_emr_log_dir> [data_size] [number_of_iterations]");
			return false;
		}

		if (inputArgs[4] == inputArgs[5]) {
			System.out.println("S3_BUCKET_DATA_FILE and EMR_LOG_DIR can't be the same. Please check your Inputs!");
			return false;
		}

		if (inputArgs[0] == null) {
			System.out.println("Please enter a valid aws username !");
			return false;
		}

		if (inputArgs[1] == null) {
			System.out.println("Please enter a valid aws secret");
			return false;
		}

		if (!Arrays.asList(CLUSTER_TYPES).contains(inputArgs[2])) {
			System.out.println("Please enter a valid cluster type:\n<cluster_types>: {\"flink\", \"spark\"}");
			return false;
		}

		if (!Arrays.asList(METRIC_TYPES).contains(inputArgs[3])) {
			System.out.println(
					"Please enter a valid metric type:\n<metric_types>: {\"groupby\", \"sorting\", \"aggregate\"}");
			return false;
		}

		if (!inputArgs[4].startsWith("s3://")) {
			System.out.println("Please enter a valid s3 bucket name:\n<s3_bucket_data_file> s3://myawsbucket/data/");
			return false;
		}

		if (!inputArgs[5].startsWith("s3://")) {
			System.out.println("Please enter a valid s3 EMR_LOG_DIR:\n<s3_bucket_names> s3://myawsbucket/log/");
			return false;
		}

		return true;
	}
}
