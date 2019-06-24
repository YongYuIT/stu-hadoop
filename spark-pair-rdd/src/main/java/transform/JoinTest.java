package transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class JoinTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JoinTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> tabName = sc.parallelize(Arrays.asList("1001 yuyong", "1002 benben", "1003 guo", "1004 fei", "1005 fei-guo"));
        JavaRDD<String> tabBook = sc.parallelize(Arrays.asList("1001 bookA", "1002 bookB", "1001 bookC", "1004 bookD", "1006 bookE"));
        PairFunction pairFunction = new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String s) throws Exception {
                String[] ss = s.split(" ");
                Tuple2<Integer, String> result = new Tuple2<Integer, String>(Integer.parseInt(ss[0]), ss[1]);
                return result;
            }
        };
        JavaPairRDD<Integer, String> KVName = tabName.mapToPair(pairFunction);
        JavaPairRDD<Integer, String> KVBook = tabBook.mapToPair(pairFunction);

        //内连接
        JavaPairRDD<Integer, Tuple2<String, String>> innerJoinResult = KVName.join(KVBook);
        for (Tuple2<Integer, Tuple2<String, String>> integerTuple2Tuple2 : innerJoinResult.collect()) {
            System.out.println("inner join ## " + integerTuple2Tuple2._1 + ":" + integerTuple2Tuple2._2._1 + "-->" + integerTuple2Tuple2._2._2);
        }

        //左外连接
        JavaPairRDD<Integer, Tuple2<String, Optional<String>>> leftOutJoinResult = KVName.leftOuterJoin(KVBook);
        for (Tuple2<Integer, Tuple2<String, Optional<String>>> integerTuple2Tuple2 : leftOutJoinResult.collect()) {
            System.out.println("leftOut join ## " + integerTuple2Tuple2._1 + ":" + integerTuple2Tuple2._2._1 + "-->" + (integerTuple2Tuple2._2._2.isPresent() ? integerTuple2Tuple2._2._2.get() : "null"));
        }

        //右外连接
        JavaPairRDD<Integer, Tuple2<Optional<String>, String>> rightOutJoinResult = KVName.rightOuterJoin(KVBook);
        for (Tuple2<Integer, Tuple2<Optional<String>, String>> integerTuple2Tuple2 : rightOutJoinResult.collect()) {
            System.out.println("rightOut join ## " + integerTuple2Tuple2._1 + ":" + (integerTuple2Tuple2._2._1.isPresent() ? integerTuple2Tuple2._2._1.get() : "null") + "-->" + integerTuple2Tuple2._2._2);
        }

        //排序
        for (Tuple2<Integer, Tuple2<Optional<String>, String>> integerTuple2Tuple2 : rightOutJoinResult.sortByKey().collect()) {
            System.out.println("rightOut join #### " + integerTuple2Tuple2._1 + ":" + (integerTuple2Tuple2._2._1.isPresent() ? integerTuple2Tuple2._2._1.get() : "null") + "-->" + integerTuple2Tuple2._2._2);
        }
        for (Tuple2<Integer, Tuple2<Optional<String>, String>> integerTuple2Tuple2 : rightOutJoinResult.sortByKey(false).collect()) {
            System.out.println("rightOut join ###### " + integerTuple2Tuple2._1 + ":" + (integerTuple2Tuple2._2._1.isPresent() ? integerTuple2Tuple2._2._1.get() : "null") + "-->" + integerTuple2Tuple2._2._2);
        }
    }

    /*
     * $ hadoop dfsadmin -report
     * $ echo $HADOOP_CONF_DIR
     * $ spark-submit --master yarn --class transform.JoinTest /home/yong/stu-hadoop/spark-pair-rdd/target/spark-pair-rdd-1.0-SNAPSHOT.jar
     */
}
