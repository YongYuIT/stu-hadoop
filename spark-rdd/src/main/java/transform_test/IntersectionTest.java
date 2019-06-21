package transform_test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class IntersectionTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("transform_test.IntersectionTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 1, 2, 2, 3, 3, 4, 5, 6));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 1, 2, 5, 6, 7, 7, 8, 9));
        //IntersectionTest返回两个RDD中都有的数据，也会导致全网数据混洗，开销大
        JavaRDD<Integer> intersectionResult = rdd1.intersection(rdd2);
        for (Integer integer : intersectionResult.collect()) {
            System.out.println("intersection----------------->" + integer);
        }
        //subtract也会导致混洗
        JavaRDD<Integer> subResult = rdd1.subtract(rdd2);
        for (Integer integer : subResult.collect()) {
            System.out.println("subtract----------------->" + integer);
        }
        //求笛卡尔积，开销巨大
        JavaPairRDD<Integer, Integer> carResult = rdd1.cartesian(rdd2);
        for (Tuple2<Integer, Integer> integerIntegerTuple2 : carResult.collect()) {
            System.out.println("cartesian----------------->" + integerIntegerTuple2._1 + "-->" + integerIntegerTuple2._2);
        }
    }
}
/*
 * $ hadoop dfsadmin -report
 * $ echo $HADOOP_CONF_DIR
 * $ spark-submit --master yarn --class transform_test.IntersectionTest /home/yong/stu-hadoop/spark-rdd/target/spark-rdd-1.0-SNAPSHOT.jar
 */