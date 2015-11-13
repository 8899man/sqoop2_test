package io.transwarp;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.*;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.validation.Status;

import org.apache.sqoop.submission.counter.Counters;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Created by qls on 9/1/15.
 */
public class mysqlToHdfs {

    public static SqoopClient client = null;
    public static Properties props = new Properties();

    public static void main(String[] args) {

        if(args.length != 1){
            System.out.println("Usage: conf.properties <configFile>\n");
            System.exit(1);
        }
        try {
            props.load(new FileInputStream(args[0]));
        } catch (IOException e) {
            e.printStackTrace();
        }

        printUsargs();

        //init sqoopClient
        initClient();

        //clean all the information before run
        cleanJob();

        // Create Source Connection JDBC
        long fromConnectorId = 2;
        MLink fromLink = createMysqlLink(fromConnectorId);

        //Create Source Connection HDFS
        long toConnectorId = 1;
        MLink toLink = createHDFSLink(toConnectorId);

        //create new job
        long fromLinkId = fromLink.getPersistenceId();
        long toLinkId = toLink.getPersistenceId();
        MJob job = createJob(fromLinkId, toLinkId);

        //run job
        long jobId = job.getPersistenceId();
        MSubmission submission = client.startJob(jobId);
        System.out.println("please wait 5s");
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("JOB status : " + submission.getStatus());
        while (submission.getStatus().isRunning() && submission.getProgress() != -1) {
            System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
            //report every 3s
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //System.out.println("Job over... ...");
        System.out.println("Hadoop job ID :" + submission.getExternalId());
        Counters counters = submission.getCounters();
        if (counters != null) {
            System.out.println("counter:");
            for (CounterGroup group : counters) {
                System.out.print("\t");
                System.out.println(group.getName());
                for (Counter counter : group) {
                    System.out.print("\t\t");
                    System.out.print(counter.getName());
                    System.out.print(": ");
                    System.out.println(counter.getValue());
                }
            }
        }
        if (submission.getExceptionInfo() != null) {
            System.out.println("JOB Exception info : " + submission.getExceptionInfo());
        }
        System.out.println("MySQL to HDFS information is over ..");
    }

    private static MJob createJob(long fromLinkId, long toLinkId) {
        MJob job = client.createJob(fromLinkId, toLinkId);
        job.setName("MySQL to HDFS job");
        job.setCreationUser("QLS");
        //set some information
        MFromConfig fromJobConfig = job.getFromJobConfig();
        fromJobConfig.getStringInput("fromJobConfig.schemaName").setValue(props.getProperty(sqoopUtil.SCHEMANAME,sqoopUtil.DEFAULT_SCHEMANAME));
        fromJobConfig.getStringInput("fromJobConfig.tableName").setValue(props.getProperty(sqoopUtil.TABLENAME,sqoopUtil.DEFAULT_TABLENAME));
        fromJobConfig.getStringInput("fromJobConfig.partitionColumn").setValue(props.getProperty(sqoopUtil.PARTITIONCOLUMN,sqoopUtil.DEFAULT_PARTITIONCOLUMN));
        MToConfig toJobConfig = job.getToJobConfig();
        toJobConfig.getStringInput("toJobConfig.outputDirectory").setValue(props.getProperty(sqoopUtil.OUTPUTDIR,sqoopUtil.DEFAULT_OUTPUTDIR));
//        MDriverConfig driverConfig = job.getDriverConfig();
//        driverConfig.getStringInput("throttlingConfig.numExtractors").setValue("3");

        Status status = client.saveJob(job);
        if (status.canProceed()) {
            System.out.println("JOB create sucess  ID: " + job.getPersistenceId());
        } else {
            System.out.println("JOB create fail ..");
        }

        return job;
    }

    private static MLink createMysqlLink(long fromConnectorId) {
        MLink fromLink = client.createLink(fromConnectorId);
        fromLink.setName("JDBC connector");
        fromLink.setCreationUser("QLS");
        MLinkConfig fromLinkConfig = fromLink.getConnectorLinkConfig();
        fromLinkConfig.getStringInput("linkConfig.connectionString").setValue(props.getProperty(sqoopUtil.CONNECTSTRING,sqoopUtil.DEFAULT_CONNECTSTRING));
        fromLinkConfig.getStringInput("linkConfig.jdbcDriver").setValue(props.getProperty(sqoopUtil.JDBCDRIVER,sqoopUtil.DEFAULT_JDBCDRIVER));
        fromLinkConfig.getStringInput("linkConfig.username").setValue(props.getProperty(sqoopUtil.USERNAME,sqoopUtil.DEFAULT_JDBCDRIVER));
        fromLinkConfig.getStringInput("linkConfig.password").setValue(props.getProperty(sqoopUtil.PASSWOED,sqoopUtil.DEFAULT_PASSWOED));
        Status fromStatus = client.saveLink(fromLink);
        if (fromStatus.canProceed()) {
            System.out.println("JDBC Link create sucess，ID: " + fromLink.getPersistenceId());
        } else {
            System.out.println("JDBC Link create fail");
        }
        return fromLink;
    }

    private static MLink createHDFSLink(long toConnectorId) {
        MLink toLink = client.createLink(toConnectorId);
        toLink.setName("HDFS connector");
        toLink.setCreationUser("QLS");
        MLinkConfig toLinkConfig = toLink.getConnectorLinkConfig();
        toLinkConfig.getStringInput("linkConfig.uri").setValue(props.getProperty(sqoopUtil.HDFSURL,sqoopUtil.DEFAULT_HDFSURL));
        Status toStatus = client.saveLink(toLink);
        if (toStatus.canProceed()) {
            System.out.println("HDFS Link create sucess, ID: " + toLink.getPersistenceId());
        } else {
            System.out.println("HDFS Link create fail ..");
        }
        return toLink;
    }

    private static void printUsargs() {

        for (Object str : props.keySet()) {
            if (str instanceof String) {
                System.out.println(str + " : " + props.getProperty((String) str));
            }
        }
    }

    private static void cleanJob() {
        List<MJob> jobLists = client.getJobs();
        if(jobLists !=null && jobLists.size()>0) {
            for (MJob mjob : jobLists) {
                client.deleteJob(mjob.getPersistenceId());
            }
        }

        //drop all links
        List<MLink> lists = client.getLinks();
        if(lists !=null && lists.size()>0) {
            for (MLink link : lists) {
                System.out.println(link.getPersistenceId());
                client.deleteLink(link.getPersistenceId());
            }
        }
    }

    public static void initClient() {
        String url=props.getProperty(sqoopUtil.SERVERURL,sqoopUtil.DEAFULT_SERVERURL);
        client = new SqoopClient(url);
    }
}
