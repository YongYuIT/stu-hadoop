import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class MapToPairTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("MapToPairTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> orgRdd = sc.parallelize(Arrays.asList("1 yong", "2 benben", "3 guoqing"));
        JavaPairRDD<Integer, String> jpRdd = orgRdd.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String s) throws Exception {
                String[] ss = s.split(" ");
                Tuple2<Integer, String> result = new Tuple2<Integer, String>(Integer.parseInt(ss[0]), ss[1]);
                return result;
            }
        });

        for (Tuple2<Integer, String> integerStringTuple2 : jpRdd.collect()) {
            System.out.println(integerStringTuple2._1 + "-->" + integerStringTuple2._2);
        }
    }
}

/*
 * $ hadoop dfsadmin -report
 * $ echo $HADOOP_CONF_DIR
 * $ spark-submit --master yarn --class MapToPairTest /home/yong/stu-hadoop/spark-pair-rdd/target/spark-pair-rdd-1.0-SNAPSHOT.jar
 */