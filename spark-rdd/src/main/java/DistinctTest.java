import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class DistinctTest {
    public static void main(String[] args) {
        //distinct开销很大，因为这个操作需要进行全网数据混洗才能达成
        SparkConf conf = new SparkConf().setMaster("local").setAppName("DistinctTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> orgValues = sc.parallelize(Arrays.asList("A", "A", "B", "C"));
        JavaRDD<String> result = orgValues.distinct();
        for (String s : result.collect()) {
            System.out.println("----------------->" + s);
        }
    }
}
/*
 * $ hadoop dfsadmin -report
 * $ echo $HADOOP_CONF_DIR
 * $ spark-submit --master yarn --class DistinctTest /home/yong/stu-hadoop/spark-rdd/target/spark-rdd-1.0-SNAPSHOT.jar
 */