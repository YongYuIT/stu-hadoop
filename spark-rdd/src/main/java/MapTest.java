import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class MapTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("MapTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> orgValues = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));
        JavaRDD<Float> resluts = orgValues.map(new Function<Integer, Float>() {
            public Float call(Integer v1) throws Exception {
                return ((float) v1) / 10;
            }
        });
        resluts.saveAsTextFile(args[0]);
    }
}

/*
 * $ hadoop dfsadmin -report
 * $ echo $HADOOP_CONF_DIR
 * $ hdfs dfs -rm -r /yong/spark/MapTest/*
 * $ hdfs dfs -mkdir -p /yong/spark/MapTest
 * $ spark-submit --master yarn --class MapTest /home/yong/stu-hadoop/spark-rdd/target/spark-rdd-1.0-SNAPSHOT.jar /yong/spark/MapTest/output
 * $ hdfs dfs -ls /yong/spark/MapTest/output
 * $ hdfs dfs -cat /yong/spark/MapTest/output/part-00000
 */