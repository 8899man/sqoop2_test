package io.transwarp;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.*;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;

import java.util.List;

/**
 * Created by qls on 9/1/15.
 */
public class mysqlToHdfs2 {

    public static SqoopClient client = null;
    public static String url = "http://172.16.2.27:12000/sqoop/";

    public static void main(String[] args) {
        initClient();

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



        // Create Source Connection JDBC
        long fromConnectorId = 2;
        MLink fromLink = client.createLink(fromConnectorId);
        fromLink.setName("JDBC connector");
        fromLink.setCreationUser("QLS");
        MLinkConfig fromLinkConfig = fromLink.getConnectorLinkConfig();
        fromLinkConfig.getStringInput("linkConfig.connectionString").setValue("jdbc:mysql://172.16.1.82:3306/test02");
        fromLinkConfig.getStringInput("linkConfig.jdbcDriver").setValue("com.mysql.jdbc.Driver");
        fromLinkConfig.getStringInput("linkConfig.username").setValue("qls");
        fromLinkConfig.getStringInput("linkConfig.password").setValue("1234");
        Status fromStatus = client.saveLink(fromLink);
        if (fromStatus.canProceed()) {
            System.out.println("创建JDBC Link成功，ID为: " + fromLink.getPersistenceId());
        } else {
            System.out.println("创建JDBC Link失败");
        }

        //Create Source Connection HDFS
        long toConnectorId = 1;
        MLink toLink = client.createLink(toConnectorId);
        toLink.setName("HDFS connector");
        toLink.setCreationUser("QLS");
        MLinkConfig toLinkConfig = toLink.getConnectorLinkConfig();
        toLinkConfig.getStringInput("linkConfig.uri").setValue("hdfs://172.16.2.25:8020/");
        Status toStatus = client.saveLink(toLink);
        if (toStatus.canProceed()) {
            System.out.println("创建HDFS Link成功，ID为: " + toLink.getPersistenceId());
        } else {
            System.out.println("创建HDFS Link失败");
        }

        //create new job
        long fromLinkId = fromLink.getPersistenceId();
        long toLinkId = toLink.getPersistenceId();
        MJob job = client.createJob(fromLinkId, toLinkId);
        job.setName("MySQL to HDFS job");
        job.setCreationUser("QLS");
        //set some information
        MFromConfig fromJobConfig = job.getFromJobConfig();
        fromJobConfig.getStringInput("fromJobConfig.schemaName").setValue("test02");
        fromJobConfig.getStringInput("fromJobConfig.tableName").setValue("sqoop01");
        fromJobConfig.getStringInput("fromJobConfig.partitionColumn").setValue("id");
        MToConfig toJobConfig = job.getToJobConfig();
        toJobConfig.getStringInput("toJobConfig.outputDirectory").setValue("/tmp/sqoop2_test");
//        MDriverConfig driverConfig = job.getDriverConfig();
//        driverConfig.getStringInput("throttlingConfig.numExtractors").setValue("3");

        Status status = client.saveJob(job);
        if (status.canProceed()) {
            System.out.println("JOB创建成功，ID为: " + job.getPersistenceId());
        } else {
            System.out.println("JOB创建失败。");
        }

        //run job
        long jobId = job.getPersistenceId();
        MSubmission submission = client.startJob(jobId);
        System.out.println("JOB提交状态为 : " + submission.getStatus());
        while (submission.getStatus().isRunning() && submission.getProgress() != -1) {
            System.out.println("进度 : " + String.format("%.2f %%", submission.getProgress() * 100));
            //三秒报告一次进度
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("JOB执行结束... ...");
        System.out.println("Hadoop任务ID为 :" + submission.getExternalId());
        Counters counters = submission.getCounters();
        if (counters != null) {
            System.out.println("计数器:");
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
            System.out.println("JOB执行异常，异常信息为 : " + submission.getExceptionInfo());
        }
        System.out.println("MySQL通过sqoop传输数据到HDFS统计执行完毕");
    }

    public static void initClient() {
        client = new SqoopClient(url);
    }

}
