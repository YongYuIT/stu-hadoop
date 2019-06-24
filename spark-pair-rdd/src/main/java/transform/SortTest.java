package transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class SortTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SortTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> orgRdd = sc.parallelize(Arrays.asList("yong 1", "ben 22", "ben 333", "yong 4444", "guo 55555", "yong 666666", "guo 12345678"));
        PairFunction pairFunction = new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] ss = s.split(" ");
                Tuple2<String, Integer> result = new Tuple2<String, Integer>(ss[0], Integer.parseInt(ss[1]));
                return result;
            }
        };
        JavaPairRDD<String, Integer> jpRdd = orgRdd.mapToPair(pairFunction);
        //排序
        for (Tuple2<String, Integer> integerTuple2Tuple2 : jpRdd.sortByKey().collect()) {
            System.out.println("sortByKey ## " + integerTuple2Tuple2._1 + ":" + integerTuple2Tuple2._2);
        }
        for (Tuple2<String, Integer> integerTuple2Tuple2 : jpRdd.sortByKey(false).collect()) {
            System.out.println("sortByKey false ## " + integerTuple2Tuple2._1 + ":" + integerTuple2Tuple2._2);
        }

        //自定义比较器
        for (Tuple2<String, Integer> integerTuple2Tuple2 : jpRdd.sortByKey(new MyComparator()).collect()) {
            System.out.println("sortByKey comparator ## " + integerTuple2Tuple2._1 + ":" + integerTuple2Tuple2._2);
        }
    }
    /*
     * $ hadoop dfsadmin -report
     * $ echo $HADOOP_CONF_DIR
     * $ spark-submit --master yarn --class transform.SortTest /home/yong/stu-hadoop/spark-pair-rdd/target/spark-pair-rdd-1.0-SNAPSHOT.jar
     */
}


class MyComparator implements Comparator<String>, Serializable {
    private static final List<String> index = Arrays.asList("yong", "ben", "guo");

    public int compare(String o1, String o2) {
        return index.indexOf(o2) - index.indexOf(o1);
    }
}