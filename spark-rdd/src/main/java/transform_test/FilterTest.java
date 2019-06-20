package transform_test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class FilterTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("transform_test.FilterTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> words = sc.parallelize(Arrays.asList("Hello", "Hi", "this", "main", "must", "shell", "java"));
        JavaRDD<String> HWords = words.filter(new Function<String, Boolean>() {
            public Boolean call(String v1) throws Exception {
                return v1.trim().toLowerCase().startsWith("h");
            }
        });

        JavaRDD<String> MWords = words.filter(new Function<String, Boolean>() {
            public Boolean call(String v1) throws Exception {
                return v1.trim().toLowerCase().startsWith("m");
            }
        });

        JavaRDD<String> result = HWords.union(MWords);
        for (String s : result.collect()) {//collect操作将导致计算（将RDD全量搜集到驱动器进程中），全量提取，将会消耗巨大内存
            System.out.println("--------->" + s);
        }
        result.saveAsTextFile(args[0]);
    }
}

/*
 * $ hadoop dfsadmin -report
 * $ echo $HADOOP_CONF_DIR
 * $ hdfs dfs -rm -r /yong/spark/transform_test.FilterTest/*
 * $ hdfs dfs -mkdir -p /yong/spark/transform_test.FilterTest
 * $ spark-submit --master yarn --class transform_test.FilterTest /home/yong/stu-hadoop/spark-rdd/target/spark-rdd-1.0-SNAPSHOT.jar /yong/spark/transform_test.FilterTest/output
 * $ hdfs dfs -ls /yong/spark/transform_test.FilterTest/output
 * $ hdfs dfs -cat /yong/spark/transform_test.FilterTest/output/part-00000
 */