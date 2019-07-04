package com.thinking;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.lang.reflect.Field;

public class DataToHBaseSimplePut {
    /*
      $ hbase shell
      > list
      > create 'student',{NAME=>'basic_info'},{NAME=>'more_info'}
      > scan 'student'
    */
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("com.thinking.DataToHBaseSimplePut");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(args[0]);
        JavaRDD<Student> students = input.map(new Function<String, Student>() {
            public Student call(String v1) throws Exception {
                String[] values = v1.split("-->");
                Student stu = new Student();
                stu.name = values[0];
                stu.stuNum = values[1];
                stu.gender = values[2];
                stu.discipline = values[3];
                stu.grade = values[4];
                stu._class = values[5];
                stu.birthplace = values[6];
                System.out.println("yuyong------>" + v1 + "-->" + stu.stuNum);
                return stu;
            }
        });
        JavaHBaseContext hBaseContext = new JavaHBaseContext(sc, HBaseConfiguration.create());
        TableName tableName = TableName.valueOf("student");
        hBaseContext.bulkPut(students, tableName, new Function<Student, Put>() {
            public Put call(Student v1) throws Exception {
                Put put = new Put(Bytes.toBytes(v1.stuNum));
                Field[] fields = v1.getClass().getDeclaredFields();
                for (Field field : fields) {
                    put.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes(field.getName()), Bytes.toBytes(field.get(v1).toString()));
                }
                return put;
            }
        });
        sc.close();
    }

    static class Student {
        public String name;
        public String stuNum;
        public String discipline;
        public String gender;
        public String _class;
        public String birthplace;
        public String grade;
    }
}

/*
COMPILATION ERROR :
cannot access org.apache.spark.streaming.api.java.JavaDStream
  class file for org.apache.spark.streaming.api.java.JavaDStream not found

add dependency to pom :
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming_2.12</artifactId>
            <version>2.4.3</version>
            <scope>provided</scope>
        </dependency>
$ hadoop dfsadmin -report
$ echo $HADOOP_CONF_DIR
$ hdfs dfs -rm -r /user/yong/input/com.thinking.DataToHBaseSimplePut/output
$ hdfs dfs -ls /user/yong/input/com.thinking.DataToHBaseSimplePut/output
$ spark-submit --master yarn --class com.thinking.DataToHBaseSimplePut /home/yong/stu-hadoop/saprk-data-to-hbase/target/saprk-data-to-hbase-1.0-SNAPSHOT-jar-with-dependencies.jar /user/yong/input/com.thinking.DataToHBaseSimplePut/*
Caused by: java.lang.ClassNotFoundException: org.apache.hadoop.hbase.fs.HFileSystem
$ spark-submit --jars $HBASE_HOME/lib/hbase-server-2.1.5.jar \
--master yarn --class com.thinking.DataToHBaseSimplePut /home/yong/stu-hadoop/saprk-data-to-hbase/target/saprk-data-to-hbase-1.0-SNAPSHOT-jar-with-dependencies.jar /user/yong/input/com.thinking.DataToHBaseSimplePut/*
Caused by: java.lang.ClassNotFoundException: org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
$ spark-submit --jars $HBASE_HOME/lib/hbase-server-2.1.5.jar,\
$HBASE_HOME/lib/hbase-mapreduce-2.1.5.jar \
--master yarn --class com.thinking.DataToHBaseSimplePut /home/yong/stu-hadoop/saprk-data-to-hbase/target/saprk-data-to-hbase-1.0-SNAPSHOT-jar-with-dependencies.jar /user/yong/input/com.thinking.DataToHBaseSimplePut/*

由于项目后期在pom里加入了对hbase-mapreduce的引用，所以运行以下命令也可以
$ spark-submit --jars $HBASE_HOME/lib/hbase-server-2.1.5.jar \
--master yarn --class com.thinking.DataToHBaseSimplePut /home/yong/stu-hadoop/saprk-data-to-hbase/target/saprk-data-to-hbase-1.0-SNAPSHOT-jar-with-dependencies.jar /user/yong/input/com.thinking.DataToHBaseSimplePut/*
*/