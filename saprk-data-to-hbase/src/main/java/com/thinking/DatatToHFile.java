package com.thinking;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class DatatToHFile {
    /*
    $ hbase shell
    > list
    > create 'student2',{NAME=>'basic_info'},{NAME=>'more_info'}
    > scan 'student2'
    $ cd src/main/resources
    $ mkdir hbase
    $ cp $HBASE_HOME/conf/hbase-site.xml hbase/
    */
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("com.thinking.DataToHBaseSimplePut");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(args[0]);
        JavaPairRDD<String, String[]> rowCols = input.mapToPair(new PairFunction<String, String, String[]>() {
            public Tuple2<String, String[]> call(String s) throws Exception {
                String[] values = s.split("-->");
                return new Tuple2<String, String[]>(values[1], values);
            }
        }).sortByKey();
        JavaPairRDD<ImmutableBytesWritable, KeyValue> hfileRdd = rowCols.mapToPair(new PairFunction<Tuple2<String, String[]>, ImmutableBytesWritable, KeyValue>() {
            public Tuple2<ImmutableBytesWritable, KeyValue> call(Tuple2<String, String[]> stringTuple2) throws Exception {
                KeyValue kv = new KeyValue(Bytes.toBytes(stringTuple2._2()[1]), Bytes.toBytes("basic_info"), Bytes.toBytes("name"), Bytes.toBytes(stringTuple2._2()[0]));
                Tuple2 row_col = new Tuple2<ImmutableBytesWritable, KeyValue>(new ImmutableBytesWritable(Bytes.toBytes(stringTuple2._2()[1])), kv);
                return row_col;
            }
        });

        String tabName="student2";
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.mapreduce.hfileoutputformat.table.name ", tabName);
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("conn success!");
        Admin admin = connection.getAdmin();
        System.out.println("get admin success!-->" + admin.getMaster().getHostname());
        String hdfsPath = "hdfs://localhost/user/yong/input/com.thinking.DatatToHFile/OutFile";
        hfileRdd.saveAsNewAPIHadoopFile(hdfsPath, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, configuration);
        LoadIncrementalHFiles bulkLoader = new LoadIncrementalHFiles(configuration);
        TableName tableName = TableName.valueOf(tabName);
        Table table = connection.getTable(tableName);
        RegionLocator regionLocator = connection.getRegionLocator(tableName);
        bulkLoader.doBulkLoad(new Path(hdfsPath), admin, table, regionLocator);

    }
}

/*

$ hadoop dfsadmin -report
$ echo $HADOOP_CONF_DIR
$ hdfs dfs -mkdir -p /user/yong/input/com.thinking.DatatToHFile
$ hdfs dfs -put /home/yong/stu-hadoop/saprk-data-to-hbase/test_data/* /user/yong/input/com.thinking.DatatToHFile
$ hdfs dfs -ls /user/yong/input/com.thinking.DatatToHFile
$ hdfs dfs -rm -r /user/yong/input/com.thinking.DatatToHFile/OutFile
$ hdfs dfs -ls /user/yong/input/com.thinking.DatatToHFile/OutFile
$ spark-submit --jars $HBASE_HOME/lib/hbase-server-2.1.5.jar \
--master yarn --class com.thinking.DatatToHFile /home/yong/stu-hadoop/saprk-data-to-hbase/target/saprk-data-to-hbase-1.0-SNAPSHOT-jar-with-dependencies.jar /user/yong/input/com.thinking.DatatToHFile/*
*/

