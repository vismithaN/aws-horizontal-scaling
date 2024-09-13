package horizontal;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.ini4j.Ini;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.waiters.Ec2Waiter;
import utilities.Configuration;
import utilities.HttpRequest;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.regions.Region;

import static software.amazon.awssdk.services.ec2.model.ResourceType.SECURITY_GROUP;
import static software.amazon.awssdk.services.ec2.model.ResourceType.VPC;


/**
 * Class for Task1 Solution.
 */
public final class HorizontalScaling {
    /**
     * Project Tag value.
     */
    public static final String PROJECT_VALUE = "vm-scaling";

    /**
     * Configuration file.
     */
    private static final Configuration CONFIGURATION =
            new Configuration("horizontal-scaling-config.json");

    /**
     * Load Generator AMI.
     */
    private static final String LOAD_GENERATOR =
            CONFIGURATION.getString("load_generator_ami");
    
    /**
     * Web Service AMI.
     */
    private static final String WEB_SERVICE =
            CONFIGURATION.getString("web_service_ami");

    /**
     * Instance Type Name.
     */
    private static final String INSTANCE_TYPE =
            CONFIGURATION.getString("instance_type");

    /**
     * Web Service Security Group Name.
     */
    private static final String WEB_SERVICE_SECURITY_GROUP =
            "web-service-security-group";

    /**
     * Load Generator Security Group Name.
     */
    private static final String LG_SECURITY_GROUP =
            "lg-security-group";

    /**
     * HTTP Port.
     */
    private static final Integer HTTP_PORT = 80;

    /**
     * Launch Delay in milliseconds.
     */
    private static final long LAUNCH_DELAY = 100000;

    /**
     * RPS target to stop provisioning.
     */
    private static final float RPS_TARGET = 50;

    /**
     * Delay before retrying API call.
     */
    public static final int RETRY_DELAY_MILLIS = 100;

    public static final String TAG_KEY = "project";
    public static final String TAG_VALUE = "vm-scaling";
    /**
     * Logger.
     */
    private static Logger logger =
            LoggerFactory.getLogger(HorizontalScaling.class);

    /**
     * Private Constructor.
     */
    private HorizontalScaling() {
    }

    /**
     * Task1 main method.
     *
     * @param args No Args required
     * @throws Exception when something unpredictably goes wrong.
     */
    public static void main(final String[] args) throws Exception {
        // BIG PICTURE: Provision resources to achieve horizontal scalability
        //  - Create security groups for Load Generator and Web Service
        //  - Provision a Load Generator instance
        //  - Provision a Web Service instance
        //  - Register Web Service DNS with Load Generator
        //  - Add Web Service instances to Load Generator 
        //  - Terminate resources

        AwsCredentialsProvider credentialsProvider =
                DefaultCredentialsProvider.builder().build();

        // Create an Amazon EC2 Client
        Ec2Client ec2 = Ec2Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(credentialsProvider)
                .build();

        // Get the default VPC
        Vpc vpc = getDefaultVPC(ec2);
        System.out.printf("Default VPC returned : %s", vpc.vpcId());
        // Create Security Groups in the default VPC
        String lgSecurityGroupId =
                getOrCreateHttpSecurityGroup(ec2, LG_SECURITY_GROUP, vpc.vpcId());
        String wsSecurityGroupId =
                getOrCreateHttpSecurityGroup(ec2, WEB_SERVICE_SECURITY_GROUP, vpc.vpcId());

        // TODO: Create Load Generator instance and obtain DNS
        // TODO: Tag instance using Tag Specification

        String loadGeneratorDNS = createInstance(ec2, lgSecurityGroupId, LOAD_GENERATOR);

        // TODO: Create first Web Service instance and obtain DNS
        // TODO: Tag instance using Tag Specification
        String webServiceDNS = createInstance(ec2, wsSecurityGroupId, WEB_SERVICE);

        //Initialize test
        String response = initializeTest(loadGeneratorDNS, webServiceDNS);

        //Get TestID
        String testId = getTestId(response);

        //Save launch time
        Date lastLaunchTime = new Date();

        //Monitor LOG file
        Ini ini = getIniUpdate(loadGeneratorDNS, testId);
        while (ini == null || !ini.containsKey("Test finished")) {
            Thread.sleep(RETRY_DELAY_MILLIS);
            ini = getIniUpdate(loadGeneratorDNS, testId);
            float currentRPS = getRPS(ini);
            boolean timeForLaunch = lastLaunchTime.toInstant().plusSeconds(LAUNCH_DELAY).isBefore(Instant.now());

            if(timeForLaunch && currentRPS<50){
                String newWebDNS =  createInstance(ec2, wsSecurityGroupId, WEB_SERVICE);
                addWebServiceInstance(loadGeneratorDNS,newWebDNS,testId);
                lastLaunchTime = new Date();
            }

            // TODO: Check last launch time and RPS
            // TODO: Add New Web Service Instance if Required
        }

    }

    /**
     *
     * @param ec2
     * @param securityGroupId
     * @param amiId
     * @return DNS
     */
    private static String createInstance(Ec2Client ec2, String securityGroupId, String amiId) {
        RunInstancesRequest request = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(INSTANCE_TYPE)
                .minCount(1)
                .maxCount(1)
                .securityGroups(Collections.singletonList(securityGroupId))
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(Tag.builder().key(TAG_KEY).value(TAG_VALUE).build()) // Tagging the instance
                        .build())
                .build();
        RunInstancesResponse response = ec2.runInstances(request);
        String instanceId = response.instances().get(0).instanceId();
        String publicDnsName = response.instances().get(0).publicDnsName();

        System.out.printf("Created Instance with ID: %s", instanceId);

        // Wait for the instance to be in a running state
        waiter(ec2, instanceId);

        //Tag Network Interfaces
        tagNetworkInterfaceId(ec2, instanceId);
        return publicDnsName;
    }

    //Wait until instance is running
    private static void waiter(Ec2Client ec2, String instanceId) {
        Ec2Waiter waiter = ec2.waiter();

        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        // Wait until the instance is running
        waiter.waitUntilInstanceRunning(request).matched().response().ifPresent(System.out::println);
        System.out.printf("Instance %s is now running.%n", instanceId);
    }

    // Get the network interface ID associated with an instance
    public static void tagNetworkInterfaceId(Ec2Client ec2, String instanceId) {

        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        DescribeInstancesResponse response = ec2.describeInstances(request);
        Instance instance = response.reservations().get(0).instances().get(0);


        InstanceNetworkInterface networkInterface = instance.networkInterfaces().get(0);
        CreateTagsRequest createTagsRequest = CreateTagsRequest.builder()
                .resources(networkInterface.networkInterfaceId())
                .tags(Tag.builder().key(TAG_KEY).value(TAG_VALUE).build()).build();

        ec2.createTags(createTagsRequest);
        System.out.printf("Tagged Network Interface with ID: %s", networkInterface.networkInterfaceId());
    }
    /**
     * Get the latest RPS.
     *
     * @param ini INI file object
     * @return RPS Value
     */
    private static float getRPS(final Ini ini) {
        float rps = 0;
        for (String key : ini.keySet()) {
            if (key.startsWith("Current rps")) {
                rps = Float.parseFloat(key.split("=")[1]);
            }
        }
        return rps;
    }

    /**
     * Get the latest test log.
     *
     * @param loadGeneratorDNS DNS Name of load generator
     * @param testId           TestID String
     * @return INI Object
     * @throws IOException on network failure
     */
    private static Ini getIniUpdate(final String loadGeneratorDNS,
                                    final String testId)
            throws IOException {
        String response = HttpRequest.sendGet(String.format(
                "http://%s/log?name=test.%s.log",
                loadGeneratorDNS,
                testId));
        File log = new File(testId + ".log");
        FileUtils.writeStringToFile(log, response, Charset.defaultCharset());
        return new Ini(log);
    }

    /**
     * Get ID of test.
     *
     * @param response Response containing LoadGenerator output
     * @return TestID string
     */
    private static String getTestId(final String response) {
        Pattern pattern = Pattern.compile("test\\.([0-9]*)\\.log");
        Matcher matcher = pattern.matcher(response);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    /**
     * Initializes Load Generator Test.
     *
     * @param loadGeneratorDNS DNS Name of load generator
     * @param webServiceDNS    DNS Name of web service
     * @return response of initialization (contains test ID)
     */
    private static String initializeTest(final String loadGeneratorDNS,
                                         final String webServiceDNS) {
        String response = "";
        boolean launchWebServiceSuccess = false;
        while (!launchWebServiceSuccess) {
            try {
                response = HttpRequest.sendGet(String.format(
                        "http://%s/test/horizontal?dns=%s",
                        loadGeneratorDNS,
                        webServiceDNS));
                logger.info(response);
                launchWebServiceSuccess = true;
            } catch (Exception e) {
                logger.info("*");
            }
        }
        return response;
    }

    /**
     * Add a Web Service vm to Load Generator.
     *
     * @param loadGeneratorDNS DNS Name of Load Generator
     * @param webServiceDNS    DNS Name of Web Service
     * @param testId           the test ID
     * @return String response
     */
    private static String addWebServiceInstance(final String loadGeneratorDNS,
                                                final String webServiceDNS,
                                                final String testId) {
        String response = "";
        boolean launchWebServiceSuccess = false;
        while (!launchWebServiceSuccess) {
            try {
                response = HttpRequest.sendGet(String.format(
                        "http://%s/test/horizontal/add?dns=%s",
                        loadGeneratorDNS,
                        webServiceDNS));
                logger.info(response);
                launchWebServiceSuccess = true;
            } catch (Exception e) {
                try {
                    Thread.sleep(RETRY_DELAY_MILLIS);
                    Ini ini = getIniUpdate(loadGeneratorDNS, testId);
                    if (ini.containsKey("Test finished")) {
                        launchWebServiceSuccess = true;
                        logger.info("New WS is not added because the test already completed");
                    }
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
        }
        return response;
    }

    /**
     * Get the default VPC.
     * <p>
     * With EC2-Classic, your instances run in a single, flat network that you share with other customers.
     * With Amazon VPC, your instances run in a virtual private cloud (VPC) that's logically isolated to your AWS account.
     * <p>
     * The EC2-Classic platform was introduced in the original release of Amazon EC2.
     * If you created your AWS account after 2013-12-04, it does not support EC2-Classic,
     * so you must launch your Amazon EC2 instances in a VPC.
     * <p>
     * By default, when you launch an instance, AWS launches it into your default VPC.
     * Alternatively, you can create a non-default VPC and specify it when you launch an instance.
     *
     * @param ec2 EC2 Client
     * @return the default VPC object
     */
    public static Vpc getDefaultVPC(final Ec2Client ec2) {
        //TODO: Remove the exception
        //TODO: Get the default VPC

        DescribeVpcsRequest describeVpcsRequest = DescribeVpcsRequest.builder().build();
        DescribeVpcsResponse response = ec2.describeVpcs(describeVpcsRequest);
        List<Vpc> vpcList = response.vpcs();
        for(Vpc vpc : vpcList) {
            if(vpc.isDefault())
                return vpc;
        }
        return null;

    }

    /**
     * Get or create a security group and allow all HTTP inbound traffic.
     *
     * @param ec2               EC2 Client
     * @param securityGroupName the name of the security group
     * @param vpcId             the ID of the VPC
     * @return ID of security group
     */
    public static String getOrCreateHttpSecurityGroup(final Ec2Client ec2,
                                                      final String securityGroupName,
                                                      final String vpcId) {

        try {
            // Check if the Security group already exists, if already exists then return the groupID
            DescribeSecurityGroupsRequest request = DescribeSecurityGroupsRequest.builder()
                    .groupNames(securityGroupName).build();
            DescribeSecurityGroupsResponse response = ec2.describeSecurityGroups(request);
            List<String> groupIds = response.securityGroups().stream().map(SecurityGroup::groupId).collect(Collectors.toList());
            if (!groupIds.isEmpty()) {
                System.out.printf("Security group %s already exists with ID: %s", securityGroupName, groupIds.get(0));
                return groupIds.get(0);
            }
         }catch(Ec2Exception ex) {
            //Create new Security group
            CreateSecurityGroupRequest createSecurityGroupRequest = CreateSecurityGroupRequest.builder()
                    .groupName(securityGroupName)
                    .description("Load Generator Security group")
                    .vpcId(vpcId)
                    .tagSpecifications(TagSpecification.builder()
                            .resourceType(SECURITY_GROUP)
                            .tags(Tag.builder().key(TAG_KEY).value(TAG_VALUE).build())
                            .build())
                    .build();
            CreateSecurityGroupResponse createSecurityGroupResponse = ec2.createSecurityGroup(createSecurityGroupRequest);

            //Allow HTTP inbound traffic for the security group
            IpPermission ipPermission = IpPermission.builder()
                    .fromPort(HTTP_PORT)
                    .toPort(HTTP_PORT)
                    .ipProtocol("tcp")
                    .ipRanges(IpRange.builder().cidrIp("0.0.0.0/0").build())
                    .build();

            AuthorizeSecurityGroupIngressRequest ingressRequest = AuthorizeSecurityGroupIngressRequest.builder()
                    .groupId(createSecurityGroupResponse.groupId())
                    .ipPermissions(ipPermission).build();
            ec2.authorizeSecurityGroupIngress(ingressRequest);
            System.out.printf("Successfully created security group: %s", securityGroupName);
            return createSecurityGroupResponse.groupId();
        }
        return null;
    }



    /**
     * Get instance object by ID.
     *
     * @param ec2        EC2 client instance
     * @param instanceId instance ID
     * @return Instance Object
     */
    public static Instance getInstance(final Ec2Client ec2,
                                       final String instanceId) {
        //TODO: Remove the exception
        //TODO: Get an Ec2 instance
        throw new UnsupportedOperationException();
    }
}
