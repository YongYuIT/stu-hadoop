package com.thinking;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;

public class DataToHBaseBatchPut {
    /*
      $ hbase shell
      > list
      > create 'student1',{NAME=>'basic_info'},{NAME=>'more_info'}
      > scan 'student1'
    */
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("com.thinking.DataToHBaseBatchPut");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(args[0]);
        JavaRDD<Put> studentPuts = input.map(new Function<String, Put>() {
            public Put call(String v1) throws Exception {
                String[] values = v1.split("-->");
                Put put = new Put(Bytes.toBytes(values[1]));
                put.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("name"), Bytes.toBytes(values[0]));
                put.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("stuNum"), Bytes.toBytes(values[1]));
                put.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("gender"), Bytes.toBytes(values[2]));
                put.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("discipline"), Bytes.toBytes(values[3]));
                put.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("grade"), Bytes.toBytes(values[4]));
                put.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("_class"), Bytes.toBytes(values[5]));
                put.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("birthplace"), Bytes.toBytes(values[6]));
                return put;
            }
        });
        JavaHBaseContext hBaseContext = new JavaHBaseContext(sc, HBaseConfiguration.create());
        hBaseContext.foreachPartition(studentPuts, new VoidFunction<Tuple2<Iterator<Put>, Connection>>() {
            public void call(Tuple2<Iterator<Put>, Connection> iteratorConnectionTuple2) throws Exception {
                TableName tableName = TableName.valueOf("student1");
                Table table = iteratorConnectionTuple2._2().getTable(tableName);
                BufferedMutator mutator = iteratorConnectionTuple2._2().getBufferedMutator(tableName);
                while (iteratorConnectionTuple2._1().hasNext()) {
                    Put put = iteratorConnectionTuple2._1().next();
                    mutator.mutate(put);
                }
                mutator.flush();
                mutator.close();
                table.close();
            }
        });
        sc.close();
    }
}

/*

$ hadoop dfsadmin -report
$ echo $HADOOP_CONF_DIR
$ hdfs dfs -mkdir -p /user/yong/input/com.thinking.DataToHBaseBatchPut
$ hdfs dfs -put /home/yong/stu-hadoop/saprk-data-to-hbase/test_data/* /user/yong/input/com.thinking.DataToHBaseBatchPut
$ hdfs dfs -ls /user/yong/input/com.thinking.DataToHBaseBatchPut
$ hdfs dfs -rm -r /user/yong/input/com.thinking.DataToHBaseBatchPut/output
$ hdfs dfs -ls /user/yong/input/com.thinking.DataToHBaseBatchPut/output
$ spark-submit --jars $HBASE_HOME/lib/hbase-server-2.1.5.jar,\
$HBASE_HOME/lib/hbase-mapreduce-2.1.5.jar \
--master yarn --class com.thinking.DataToHBaseBatchPut /home/yong/stu-hadoop/saprk-data-to-hbase/target/saprk-data-to-hbase-1.0-SNAPSHOT-jar-with-dependencies.jar /user/yong/input/com.thinking.DataToHBaseBatchPut/*
*/
