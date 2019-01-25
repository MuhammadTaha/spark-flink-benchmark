package benchmark.Workload;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

import java.util.List;

public class ClusterManager implements IClusterManager {

    private String inputFile;
    private String JARFile;
    private String AWSKey;
    private String AWSSecret;
    private String EMRLogDir;
    private String CLUSTER_TYPE;

    public ClusterManager(String metricType, String inputFile, String AWSKey, String AWSSecret, String EMRLogDir, String CLUSTER_TYPE) {
        this.inputFile = inputFile;
        this.EMRLogDir = EMRLogDir;
        this.CLUSTER_TYPE = CLUSTER_TYPE;

        this.JARFile = getJarFile(metricType, CLUSTER_TYPE);
        this.AWSKey = AWSKey;
        this.AWSSecret = AWSSecret;
    }

    /**
     * returns the specific jar metric jar file for a given cluster type
     * @param metricType : groupby or sorting or aggregation
     * @param clusterType : Flink or Spark
     * @return
     */
    private String getJarFile(String metricType, String clusterType) {

        if (metricType.equals("groupby")) {
            return clusterType.toLowerCase() != "spark" ? METRIC_FLINK_GRB : METRIC_SPARK_GRB;
        } else if (metricType.equals("sorting")) {
            return clusterType.toLowerCase() != "spark" ? METRIC_FLINK_SOR : METRIC_SPARK_SOR;
        } else {
            return clusterType.toLowerCase() != "spark" ? METRIC_FLINK_AGG : METRIC_SPARK_AGG;
        }
    }

    @Override
    public long startBenchmark() {
        System.out.println("Starting " + this.CLUSTER_TYPE + " cluster");
        AWSCredentials credentials = new BasicAWSCredentials(this.AWSKey, this.AWSSecret);
        AmazonElasticMapReduceClient emr = new AmazonElasticMapReduceClient(credentials);
        Region euEast1 = Region.getRegion(Regions.US_EAST_1);
        emr.setRegion(euEast1);

        StepFactory stepFactory = new StepFactory();
        StepConfig enabledebugging = new StepConfig()
                .withName("Enable debugging")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(stepFactory.newEnableDebuggingStep());

        //Define cluster step
        HadoopJarStepConfig clusterJarStepConf = getClusterJarStepConfig(this.CLUSTER_TYPE);
        StepConfig clusterStepConf = new StepConfig()
                .withName(this.CLUSTER_TYPE + " step")
                .withActionOnFailure("CONTINUE")
                .withHadoopJarStep(clusterJarStepConf);

        ScriptBootstrapActionConfig s3getConfig = new ScriptBootstrapActionConfig()
                .withPath(PATH_TO_BOOTSTRAP_SCRIPT)
                .withArgs(PATH_TO_JAR_FILES + "/" + this.JARFile);
        BootstrapActionConfig s3get = new
                BootstrapActionConfig()
                .withScriptBootstrapAction(s3getConfig)
                .withName("Bootstrap");
        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("Cluster_Name_" + this.CLUSTER_TYPE)
                .withReleaseLabel("emr-5.20.0")
                .withBootstrapActions(s3get)
                .withSteps(enabledebugging, clusterStepConf)
                .withApplications(new Application().withName(this.CLUSTER_TYPE))
                .withLogUri(this.EMRLogDir)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withInstances(new JobFlowInstancesConfig()
                        //.withEc2KeyName("key_virginia")
                        .withInstanceCount(3)
                        .withKeepJobFlowAliveWhenNoSteps(false)
                        .withMasterInstanceType("m5.xlarge")
                        .withSlaveInstanceType("m5.xlarge"));

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
            return new HadoopJarStepConfig()
                    .withJar("command-runner.jar")
                    .withArgs("spark-submit", "--executor-memory", "1g", "/home/hadoop/jarfile/" + this.JARFile, inputFile);
        } else {
            return new HadoopJarStepConfig()
                    .withJar("command-runner.jar")
                    .withArgs("flink", "run", "-m", "yarn-cluster", "-yn", "2", "/home/hadoop/jarfile/" + this.JARFile,
                            "--input", inputFile);
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
    private long getExecutionTime(AmazonElasticMapReduceClient emr, RunJobFlowRequest request, String clusterType) {
        RunJobFlowResult result = emr.runJobFlow(request);
        System.out.println("Cluster ID: " + result.getJobFlowId());

        //Get execution time
        //Get Step ID
        DescribeStepRequest step = new DescribeStepRequest();
        step.setClusterId(result.getJobFlowId());
        ListStepsRequest listSteps = new ListStepsRequest();
        listSteps.setClusterId(result.getJobFlowId());
        List<StepSummary> steps = emr.listSteps(listSteps).getSteps();
        int i = 0;
        while (i < steps.size()) {
            if (steps.get(i).getName().equals(clusterType + " step")) {
                System.out.println("(" + result.getJobFlowId() + ") Step ID: " + steps.get(i).getId());
                step.setStepId(steps.get(i).getId());
                break;
            }
            i++;
        }

        //Wait until the step is done:
        //System.out.println("Still waiting ...");
        while (!emr.describeStep(step).getStep().getStatus().getState().equals("COMPLETED")) {
            try {
                //wait a bit
                Thread.sleep(60000);
                System.out.println("(" + result.getJobFlowId() + ") Still waiting ... Current state: " + emr.describeStep(step).getStep().getStatus().getState());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //Process execution time:
        long diffInMillies = emr.describeStep(step).getStep().getStatus().getTimeline().getEndDateTime().getTime()
                - emr.describeStep(step).getStep().getStatus().getTimeline().getStartDateTime().getTime();
        System.out.println("(" + result.getJobFlowId() + ") Execution time in ms: " + diffInMillies);

        System.out.println("(" + result.getJobFlowId() + ") DONE");

        return diffInMillies;
    }

}
