import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class BuildIndexForHBase {
    public static void main(String[] args) throws Exception {
/*
Caused by: org.apache.spark.SparkException: Job aborted due to stage failure: Task 0.0 in stage 0.0 (TID 0) had a not serializable result: org.apache.hadoop.hbase.io.ImmutableBytesWritable
Serialization stack:
        - object not serializable (class: org.apache.hadoop.hbase.io.ImmutableBytesWritable, value: 68 65 69 6c 6f 6e 67 6a 69 61 6e 67)
*/
        System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkReadHBase");//echo $HADOOP_CONF_DIR
        JavaSparkContext sc = new JavaSparkContext(conf);
        Configuration hbaseConf = HBaseConfiguration.create();
        JavaHBaseContext hBaseContext = new JavaHBaseContext(sc, hbaseConf);
        Scan scan = new Scan();
        scan.setCaching(20);
        Filter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("birthplace")));
        scan.setFilter(filter);
        JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hData = hBaseContext.hbaseRDD(TableName.valueOf("student2"), scan);
        JavaPairRDD<ImmutableBytesWritable, KeyValue> hfileRdd = hData.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, ImmutableBytesWritable, KeyValue>() {
            public Tuple2<ImmutableBytesWritable, KeyValue> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                Result place = immutableBytesWritableResultTuple2._2();
                byte[] bPlace = place.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("birthplace"));
                ImmutableBytesWritable stuNum = immutableBytesWritableResultTuple2._1();
                byte[] bStuNum = stuNum.get();
                System.out.println(Bytes.toString(bStuNum) + "------>" + Bytes.toString(bPlace));
                KeyValue kv = new KeyValue(bPlace, Bytes.toBytes("birthplace_stu_info"), bStuNum, Bytes.toBytes("1"));
                Tuple2 row_col = new Tuple2<ImmutableBytesWritable, KeyValue>(new ImmutableBytesWritable(bPlace), kv);
                return row_col;
            }
        });
        hfileRdd = hfileRdd.sortByKey();

        String outTabName = "birthplace_stu_index";
        hbaseConf.set("hbase.mapreduce.hfileoutputformat.table.name ", outTabName);
        Connection connection = ConnectionFactory.createConnection(hbaseConf);
        System.out.println("conn success!");
        Admin admin = connection.getAdmin();
        System.out.println("get admin success!-->" + admin.getMaster().getHostname());
        String hdfsPath = "hdfs://localhost/user/yong/input/BuildIndexForHBase/OutFile";
        //hbase-mapreduce
        hfileRdd.saveAsNewAPIHadoopFile(hdfsPath, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, hbaseConf);
        LoadIncrementalHFiles bulkLoader = new LoadIncrementalHFiles(hbaseConf);
        TableName outTableName = TableName.valueOf(outTabName);
        Table table = connection.getTable(outTableName);
        RegionLocator regionLocator = connection.getRegionLocator(outTableName);
        bulkLoader.doBulkLoad(new Path(hdfsPath), admin, table, regionLocator);
    }
}
/*
$ hbase shell
> list
> create 'birthplace_stu_index',{NAME=>'birthplace_stu_info'},{NAME=>'more_info'}
> exit
$ hdfs dfs -rm -r /user/yong/input/BuildIndexForHBase/OutFile
$ spark-submit --master yarn --class BuildIndexForHBase /home/yong/stu-hadoop20190709001/spark-data-from-hbase/target/spark-data-from-hbase-1.0-SNAPSHOT-jar-with-dependencies.jar
*/