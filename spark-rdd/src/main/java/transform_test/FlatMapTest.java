package transform_test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;

public class FlatMapTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("transform_test.FlatMapTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> orgValues = sc.parallelize(Arrays.asList("A B", "C D", "E F"));
        JavaRDD<String> mapResult = orgValues.map(new Function<String, String>() {
            public String call(String v1) throws Exception {
                System.out.println("this is map v1-->" + v1);
                return "this is map v1-->" + v1;
            }
        });
        JavaRDD<String> flatMapReslut = orgValues.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                System.out.println("this is flatMap s-->" + s);
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        for (String s : flatMapReslut.collect()) {
            System.out.println("this is flatMap reslut s-->" + s);
        }
    }
}

/*
 * $ hadoop dfsadmin -report
 * $ echo $HADOOP_CONF_DIR
 * $ spark-submit --master yarn --class transform_test.FlatMapTest /home/yong/stu-hadoop/spark-rdd/target/spark-rdd-1.0-SNAPSHOT.jar
 */