package action_test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;

public class ReduceTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("action_test.ReduceTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 1, 2, 2, 3, 3, 4, 5, 6));
        int sum = rdd1.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println("reduce v1-->" + v1 + ",v2-->" + v2);
                return v1 + v2;
            }
        });
        System.out.println("reduce sum-->" + sum);

        sum = rdd1.fold(0, new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println("fold v1-->" + v1 + ",v2-->" + v2);
                return v1 + v2;
            }
        });
        System.out.println("fold sum-->" + sum);
    }
}
/*
 * $ hadoop dfsadmin -report
 * $ echo $HADOOP_CONF_DIR
 * $ spark-submit --master yarn --class action_test.ReduceTest /home/yong/stu-hadoop/spark-rdd/target/spark-rdd-1.0-SNAPSHOT.jar
 */