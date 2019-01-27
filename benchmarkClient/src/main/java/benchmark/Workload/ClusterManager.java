package benchmark.Workload;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.AmazonElasticMapReduceException;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepRequest;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepStatus;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

public class ClusterManager implements IClusterManager {

	private String inputFile;
	private String JARFile;
	private String AWSKey;
	private String AWSSecret;
	private String EMRLogDir;
	private String CLUSTER_TYPE;
	private int numberOfIteration;
	private File logFile;
	private PrintWriter logFileWriter;
	private final LocalDateTime ldt = LocalDateTime.now().plusDays(1);
	private final DateTimeFormatter formmat1 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

	public ClusterManager(String metricType, String inputFile, String AWSKey, String AWSSecret, String EMRLogDir,
			String CLUSTER_TYPE, int numberOfIteration) {
		this.inputFile = inputFile;
		this.EMRLogDir = EMRLogDir;
		this.CLUSTER_TYPE = CLUSTER_TYPE;

		this.JARFile = getJarFile(metricType, CLUSTER_TYPE);
		this.AWSKey = AWSKey;
		this.AWSSecret = AWSSecret;
		this.numberOfIteration = numberOfIteration;

		// 2018-05-12

		this.logFile = new File("clientLog_" + formmat1.format(LocalDateTime.now()));
		try {
			this.logFileWriter = new PrintWriter(this.logFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		this.logFileWriter.write("New Cluster Manager created. ("
				+ Arrays.toString(
						new String[] { CLUSTER_TYPE, metricType, inputFile, Integer.toString(numberOfIteration) })
				+ "\n");
	}

	/**
	 * returns the specific jar metric jar file for a given cluster type
	 * 
	 * @param metricType  : groupby or sorting or aggregation
	 * @param clusterType : Flink or Spark
	 * @return
	 */
	private String getJarFile(String metricType, String clusterType) {

		if (metricType.toLowerCase().equals("groupby")) {
			return clusterType.toLowerCase().equals("spark") ? METRIC_SPARK_GRB : METRIC_FLINK_GRB;
		} else if (metricType.toLowerCase().equals("sorting")) {
			return clusterType.toLowerCase().equals("spark") ? METRIC_SPARK_SOR : METRIC_FLINK_SOR;
		} else {
			return clusterType.toLowerCase().equals("spark") ? METRIC_SPARK_AGG : METRIC_FLINK_AGG;
		}
	}

	@Override
	public long startBenchmark() {
		this.logFileWriter.write("Starting Benchmark.\n");
		System.out.println("Starting " + this.CLUSTER_TYPE + " cluster");
		AWSCredentials credentials = new BasicAWSCredentials(this.AWSKey, this.AWSSecret);
		AmazonElasticMapReduceClient emr = new AmazonElasticMapReduceClient(credentials);
		Region euEast1 = Region.getRegion(Regions.US_EAST_1);
		emr.setRegion(euEast1);

		StepFactory stepFactory = new StepFactory();
		StepConfig enabledebugging = new StepConfig().withName("Enable debugging")
				.withActionOnFailure("TERMINATE_JOB_FLOW").withHadoopJarStep(stepFactory.newEnableDebuggingStep());

		// Define cluster step

		StepConfig[] stepConfigs = new StepConfig[numberOfIteration + 1];
		stepConfigs[0] = enabledebugging;
		for (int i = 0; i < numberOfIteration; i++) {
			HadoopJarStepConfig clusterJarStepConf = getClusterJarStepConfig(this.CLUSTER_TYPE);
			stepConfigs[i + 1] = new StepConfig().withName(this.CLUSTER_TYPE + " step " + i)
					.withActionOnFailure("CONTINUE").withHadoopJarStep(clusterJarStepConf);
		}

		ScriptBootstrapActionConfig s3getConfig = new ScriptBootstrapActionConfig().withPath(PATH_TO_BOOTSTRAP_SCRIPT)
				.withArgs(PATH_TO_JAR_FILES + "/" + this.JARFile);
		BootstrapActionConfig s3get = new BootstrapActionConfig().withScriptBootstrapAction(s3getConfig)
				.withName("Bootstrap");
		RunJobFlowRequest request = new RunJobFlowRequest().withName("Cluster_Name_" + this.CLUSTER_TYPE)
				.withReleaseLabel("emr-5.20.0").withBootstrapActions(s3get).withSteps(stepConfigs)
				.withApplications(new Application().withName(this.CLUSTER_TYPE)).withLogUri(this.EMRLogDir)
				.withServiceRole("EMR_DefaultRole").withJobFlowRole("EMR_EC2_DefaultRole")
				.withInstances(new JobFlowInstancesConfig()
						// .withEc2KeyName("key_virginia")
						.withInstanceCount(3).withKeepJobFlowAliveWhenNoSteps(false).withMasterInstanceType("m5.xlarge")
						.withSlaveInstanceType("m5.xlarge"));

		this.logFileWriter.write("Request performed.\n");
		return getExecutionTime(emr, request, this.CLUSTER_TYPE);

	}

	/**
	 * Creates a specific HadoopJarStepConfig denpending on the given cluster type
	 *
	 * @param clusterType
	 * @return
	 */
	private HadoopJarStepConfig getClusterJarStepConfig(String clusterType) {

		if (clusterType.equals("spark")) {
			return new HadoopJarStepConfig().withJar("command-runner.jar").withArgs("spark-submit", "--executor-memory",
					"10g", "/home/hadoop/jarfile/" + this.JARFile, inputFile);
		} else {
			return new HadoopJarStepConfig().withJar("command-runner.jar").withArgs("flink", "run", "-m",
					"yarn-cluster", "-yn", "2", "/home/hadoop/jarfile/" + this.JARFile, "--input", inputFile);
		}
	}

	/**
	 * Fetch execution time of EMR
	 *
	 * @param emr
	 * @param request
	 * @param clusterType
	 * @return
	 */
	private Long getExecutionTime(AmazonElasticMapReduceClient emr, RunJobFlowRequest request, String clusterType) {
		RunJobFlowResult result = emr.runJobFlow(request);
		System.out.println("Cluster ID: " + result.getJobFlowId());

		// Get execution time
		// Get Step ID

		ListStepsRequest listSteps = new ListStepsRequest();
		listSteps.setClusterId(result.getJobFlowId());
		List<StepSummary> steps = emr.listSteps(listSteps).getSteps();
		steps = Lists.reverse(steps);

		List<Long> diffs = Lists.newArrayList();

		int i = 0;
		this.logFileWriter.write("Waiting for measurement results...\n");
		this.logFileWriter.flush();
		while (i < steps.size()) {
			if (steps.get(i).getName().startsWith(clusterType + " step")) {
				System.out.println("(" + result.getJobFlowId() + ") Step ID: " + steps.get(i).getId() + " Name: "
						+ steps.get(i).getName());
				DescribeStepRequest step = new DescribeStepRequest();
				step.setClusterId(result.getJobFlowId());
				step.setStepId(steps.get(i).getId());
				
				boolean completed = false;
				int retries = 1;

				// Wait until the step is done:
				while (!completed) {
					try {
						StepStatus status;
						status = emr.describeStep(step).getStep().getStatus();
						completed = status.getState().equals("COMPLETED");
						if (completed) {
							// Process execution time:
							long diffInMillies = status.getTimeline().getEndDateTime().getTime()
									- status.getTimeline().getStartDateTime()
											.getTime();
							diffs.add(diffInMillies);
							System.out.println(formmat1.format(LocalDateTime.now()) + " (" + result.getJobFlowId()
									+ ") Execution time in ms: " + diffInMillies);
							this.logFileWriter.write(emr.describeStep(step).getStep().getStatus().getTimeline()
									.getStartDateTime().getTime() + ", "
									+ emr.describeStep(step).getStep().getStatus().getTimeline().getEndDateTime()
											.getTime()
									+ ", " + diffInMillies + "\n");
						} else {
							// wait a bit
							Thread.sleep(60000 * retries);
							System.out.println("(" + result.getJobFlowId() + ") Still waiting ... Current state: "
									+ emr.describeStep(step).getStep().getStatus().getState() + "(waiting " + retries
									+ " minutes before reattempting)");
						}
					} catch (AmazonElasticMapReduceException aemre) {
						retries++;
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			i++;
		}
		this.logFileWriter.write("(" + result.getJobFlowId() + ") DONE\n");
		this.logFileWriter.flush();
		this.logFileWriter.close();
		System.out.println("(" + result.getJobFlowId() + ") DONE");
		return diffs.stream().mapToLong(l -> l.longValue()).sum();
	}
}
