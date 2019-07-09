import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Map;

public class SparkReadHBase {

    public static void main(String[] args) {
        //spark-core_2.12
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkReadHBase");//echo $HADOOP_CONF_DIR
        JavaSparkContext sc = new JavaSparkContext(conf);
        //hbase-spark hbase-client
        Configuration hbaseConf = HBaseConfiguration.create();
        Iterator<Map.Entry<String, String>> iterator = hbaseConf.iterator();
        while (iterator.hasNext()) {
            Map.Entry kv = iterator.next();
            System.out.println("-------------hbaseConf-->" + kv.getKey() + "-->" + kv.getValue());
        }
        JavaHBaseContext hBaseContext = new JavaHBaseContext(sc, hbaseConf);
        Scan scan = new Scan();
        scan.setCaching(20);
        KeyOnlyFilter kof = new KeyOnlyFilter();
        scan.setFilter(kof);
        TableName tableName = TableName.valueOf("student2");
        JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hData = hBaseContext.hbaseRDD(tableName, scan);
        Optional<Partitioner> partitionerOptional = hData.partitioner();
        System.out.println("-------------is def-->" + partitionerOptional.isPresent());
        System.out.println("--------read success-->" + hData.count());
    }

}


/*
$ hadoop dfsadmin -report
$ echo $HADOOP_CONF_DIR
$ hdfs dfs -mkdir -p /user/yong/input/SparkReadHBase
$ hdfs dfs -ls /user/yong/input/SparkReadHBase
$ spark-submit --jars $HBASE_HOME/lib/hbase-server-2.1.5.jar,\
$HBASE_HOME/lib/hbase-zookeeper-2.1.5.jar,\
$HBASE_HOME/lib/hbase-mapreduce-2.1.5.jar \
--master yarn --class SparkReadHBase /home/yong/stu-hadoop20190709001/spark-data-from-hbase/target/spark-data-from-hbase-1.0-SNAPSHOT-jar-with-dependencies.jar
*/
