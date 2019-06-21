import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class TransformTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TransformTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> orgRdd = sc.parallelize(Arrays.asList("1 yong", "1 baba", "2 benben", "2 mama", "3 guoqing"));
        JavaPairRDD<Integer, String> jpRdd = orgRdd.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String s) throws Exception {
                String[] ss = s.split(" ");
                Tuple2<Integer, String> result = new Tuple2<Integer, String>(Integer.parseInt(ss[0]), ss[1]);
                return result;
            }
        });

        //适用于普通RDD的函数同样适用于pairRDD
        JavaPairRDD<Integer, String> filterResult = jpRdd.filter(new Function<Tuple2<Integer, String>, Boolean>() {
            public Boolean call(Tuple2<Integer, String> v1) throws Exception {
                return v1._1 >= 2;
            }
        });
        for (Tuple2<Integer, String> integerStringTuple2 : filterResult.collect()) {
            System.out.println("filter result-->" + integerStringTuple2._1 + "-->" + integerStringTuple2._2);
        }

        //reduceByKey把键相同的多个键值对整合成一个键值对
        JavaPairRDD<Integer, String> reduceResult = jpRdd.reduceByKey(new Function2<String, String, String>() {
            public String call(String v1, String v2) throws Exception {
                System.out.println("reduce-->" + v1 + "####" + v2);
                return v1 + "####" + v2;//整合方式
            }
        });
        for (Tuple2<Integer, String> integerStringTuple2 : reduceResult.collect()) {
            System.out.println("reduce result-->" + integerStringTuple2._1 + "-->" + integerStringTuple2._2);
        }

        //groupByKey,跟差不多reduceByKey
        JavaPairRDD<Integer, Iterable<String>> groupResult = jpRdd.groupByKey();
        for (Tuple2<Integer, Iterable<String>> integerIterableTuple2 : groupResult.collect()) {
            String reslutStr = integerIterableTuple2._1 + "####";
            for (String s : integerIterableTuple2._2) {
                reslutStr += (s + "####");
            }
            System.out.println("group result-->" + reslutStr);
        }

        //使用mapByKey和reduceByKey计算各键的平均值
        JavaRDD<String> orgRddVal = sc.parallelize(Arrays.asList("yong 100", "ben 20", "ben 30", "yong 20", "guo 30", "yong 20"));
        JavaPairRDD<String, Integer> jpRddVal = orgRddVal.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] ss = s.split(" ");
                Tuple2<String, Integer> result = new Tuple2<String, Integer>(ss[0], Integer.parseInt(ss[1]));
                return result;
            }
        });
        JavaPairRDD<String, Float> avgResult = jpRddVal.mapValues(new Function<Integer, Tuple2<Integer, Integer>>() {
            public Tuple2 call(Integer v1) throws Exception {
                return new Tuple2(v1, 1);
            }
        }).reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                return new Tuple2<Integer, Integer>(v1._1 + v2._1, v1._2 + v2._2);
            }
        }).mapValues(new Function<Tuple2<Integer, Integer>, Float>() {
            public Float call(Tuple2<Integer, Integer> v1) throws Exception {
                return ((float) v1._1) / v1._2;
            }
        });
        for (Tuple2<String, Float> stringFloatTuple2 : avgResult.collect()) {
            System.out.println("age resulr-->" + stringFloatTuple2._1 + "-->" + stringFloatTuple2._2);
        }
    }

}

/*
 * $ hadoop dfsadmin -report
 * $ echo $HADOOP_CONF_DIR
 * $ spark-submit --master yarn --class TransformTest /home/yong/stu-hadoop/spark-pair-rdd/target/spark-pair-rdd-1.0-SNAPSHOT.jar
 */
