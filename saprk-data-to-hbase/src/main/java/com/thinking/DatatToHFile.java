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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

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

        JavaPairRDD<String, String> rowCols = input.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
            public Iterator<Tuple2<String, String>> call(String s) throws Exception {
                String[] values = s.split("-->");
                Tuple2<String, String>[] tuple2s = new Tuple2[values.length];
                tuple2s[0] = new Tuple2(values[1] + "-->name", values[0]);
                tuple2s[1] = new Tuple2(values[1] + "-->stuNum", values[1]);
                tuple2s[2] = new Tuple2(values[1] + "-->gender", values[2]);
                tuple2s[3] = new Tuple2(values[1] + "-->discipline", values[3]);
                tuple2s[4] = new Tuple2(values[1] + "-->grade", values[4]);
                tuple2s[5] = new Tuple2(values[1] + "-->_class", values[5]);
                tuple2s[6] = new Tuple2(values[1] + "-->birthplace", values[6]);
                return Arrays.asList(tuple2s).iterator();
            }
        }).sortByKey();
        JavaPairRDD<ImmutableBytesWritable, KeyValue> hfileRdd = rowCols.mapToPair(new PairFunction<Tuple2<String, String>, ImmutableBytesWritable, KeyValue>() {
            public Tuple2<ImmutableBytesWritable, KeyValue> call(Tuple2<String, String> stringTuple2) throws Exception {
                String[] keys = stringTuple2._1().split("-->");
                KeyValue kv = new KeyValue(Bytes.toBytes(keys[0]), Bytes.toBytes("basic_info"), Bytes.toBytes(keys[1]), Bytes.toBytes(stringTuple2._2()));
                Tuple2 row_col = new Tuple2<ImmutableBytesWritable, KeyValue>(new ImmutableBytesWritable(Bytes.toBytes(keys[0])), kv);
                return row_col;
            }
        });

        String tabName = "student2";
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
$ spark-submit --master yarn --class com.thinking.DatatToHFile /home/yong/stu-hadoop/saprk-data-to-hbase/target/saprk-data-to-hbase-1.0-SNAPSHOT-jar-with-dependencies.jar /user/yong/input/com.thinking.DatatToHFile/*
*/

