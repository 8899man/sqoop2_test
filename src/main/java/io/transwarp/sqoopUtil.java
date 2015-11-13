package io.transwarp;

/**
 * Created by qls on 9/2/15.
 */
public class sqoopUtil {

    //jdbc mysql oracle db2
    public static final String SERVERURL = "sqoopServerUrl";
    public static final String DEAFULT_SERVERURL = "http://localhost:12000/sqoop/";
    public static final String CONNECTSTRING = "connectionString";
    public static final String DEFAULT_CONNECTSTRING = "jdbc:mysql://172.16.1.82:3306/test02";
    public static final String JDBCDRIVER = "jdbcDriver";
    public static final String DEFAULT_JDBCDRIVER = "com.mysql.jdbc.Driver";
    public static final String USERNAME = "username";
    public static final String DEFAULT_USERNAME = "qls";
    public static final String PASSWOED = "password";
    public static final String DEFAULT_PASSWOED = "1234";
    public static final String SCHEMANAME="schemaName";
    public static final String DEFAULT_SCHEMANAME="test02";
    public static final String TABLENAME="tableName";
    public static final String DEFAULT_TABLENAME="sqoop01";
    public static final String PARTITIONCOLUMN="partitionColumn";
    public static final String DEFAULT_PARTITIONCOLUMN="id";

    //hdfs
    public static final String HDFSURL = "hdfsUrl";
    public static final String DEFAULT_HDFSURL = "hdfs://172.16.2.25:8020/";
    public static final String OUTPUTDIR = "outputDirectory";
    public static final String DEFAULT_OUTPUTDIR = "/tmp/sqoop2_test";
}
