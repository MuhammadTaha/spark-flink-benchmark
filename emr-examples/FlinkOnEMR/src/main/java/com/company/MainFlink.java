package com.company;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.BootstrapActions;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

import java.util.Collection;
import java.util.UUID;

/**
 * Created by Florian on 08.01.19.
 */
public class MainFlink {
    public static void main(String[] args) {
        //Prerequisites:
        // - Ec2KeyName in your region
        // - CLI: aws emr create-default-roles (execute this command on your local machine)
        // - S3 Bucket
        //       Bucket name: test-emr-ws1819
        //       Subfolder:
        //             - test-emr-ws1819/data + add your txt file into this subfolder
        //                  The file will be used for the word count example
        //             - test-emr-ws1819/results
        //             - test-emr-ws1819/log

        //Input:
        // - args[0]: AWS username
        // - args[1]: AWS secret for user

        AWSCredentials credentials = new BasicAWSCredentials(args[0], args[1]);
        AmazonElasticMapReduceClient emr = new  AmazonElasticMapReduceClient(credentials);
        Region euEast1= Region.getRegion(Regions.US_EAST_1);
        emr.setRegion(euEast1);

        StepFactory stepFactory = new StepFactory();
        StepConfig enabledebugging = new StepConfig()
                .withName("Enable debugging")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(stepFactory.newEnableDebuggingStep());

        //Define Flink Example
        HadoopJarStepConfig flinkWordCountConf = new HadoopJarStepConfig()
                .withJar("command-runner.jar")
                .withArgs("flink", "run", "-m", "yarn-cluster", "-yn", "2", "/home/hadoop/jarfile/WordCount.jar",
                        "--input", "s3://test-emr-ws1819/data/test.txt", "--output", "s3://test-emr-ws1819/results/" + UUID.randomUUID());
        StepConfig flinkRunWordCount = new StepConfig()
                .withName("Flink wordcount step")
                .withActionOnFailure("CONTINUE")
                .withHadoopJarStep(flinkWordCountConf);


        ScriptBootstrapActionConfig s3getConfig = new ScriptBootstrapActionConfig()
                .withPath("s3://test-emr-ws1819/JARFiles/bootstrap-actions.sh")
                .withArgs("s3://test-emr-ws1819/JARFiles/WordCount.jar");
        BootstrapActionConfig s3get = new
                BootstrapActionConfig()
                    .withScriptBootstrapAction(s3getConfig)
                    .withName("Bootstrap");
        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("Cluster_Name_Flink")
                .withReleaseLabel("emr-5.20.0")
                .withBootstrapActions(s3get)
                .withSteps(enabledebugging, flinkRunWordCount)
                .withApplications(new Application().withName("Flink"))
                .withLogUri("s3://test-emr-ws1819/log/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withInstances(new JobFlowInstancesConfig()
                        .withEc2KeyName("key_virginia")
                        .withInstanceCount(3)
                        .withKeepJobFlowAliveWhenNoSteps(false)
                        .withMasterInstanceType("m3.xlarge")
                        .withSlaveInstanceType("m3.xlarge"));

        RunJobFlowResult result = emr.runJobFlow(request);
        System.out.println("Result: " + result.toString());


        //Check the log files on AWS for the execution time

    }
}
