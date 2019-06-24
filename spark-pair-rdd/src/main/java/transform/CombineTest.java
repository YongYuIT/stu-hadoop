package transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class CombineTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CombineTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> orgRddValPart1 = sc.parallelize(Arrays.asList("yong 1", "ben 22", "ben 333", "yong 4444", "guo 55555", "yong 666666", "guo 12345678"));
        JavaRDD<String> orgRddValPart2 = sc.parallelize(Arrays.asList("jack 7777777", "Alis 88888888", "Alis 999999999", "jack 123", "guo 12345", "jack 1234567"));
        PairFunction pairFunction = new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] ss = s.split(" ");
                Tuple2<String, Integer> result = new Tuple2<String, Integer>(ss[0], Integer.parseInt(ss[1]));
                return result;
            }
        };
        JavaPairRDD<String, Integer> jpRddPart1 = orgRddValPart1.mapToPair(pairFunction);
        JavaPairRDD<String, Integer> jpRddPart2 = orgRddValPart2.mapToPair(pairFunction);
        JavaPairRDD<String, Integer> jpRdd = jpRddPart1.union(jpRddPart2);

        System.out.println("partitions-->" + jpRdd.partitions().size());
        JavaPairRDD<String, Float> avgResult = jpRdd.combineByKey(new Function<Integer, Tuple2<Integer, Integer>>() {
            public Tuple2<Integer, Integer> call(Integer v1) throws Exception {
                //第一次出现在某个分区的键，其值将会被这个函数处理，形成累加的初始值，供第二个函数进行累加
                System.out.println("Function1-->" + v1);
                return new Tuple2(v1, 1);
            }
        }, new Function2<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>>() {
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Integer v2) throws Exception {
                //并非第一次出现在某个分区的键，其值会在这个函数里面进行累加
                System.out.println("Function2-->(" + v1._1 + "," + v1._2 + ")-->" + v2);
                Tuple2<Integer, Integer> tv2 = new Tuple2(v2, 1);
                Tuple2 reslut = new Tuple2(v1._1 + tv2._1, v1._2 + tv2._2);
                return reslut;
            }
        }, new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                //各个不同分区结果进行合并
                System.out.println("Function3-->(" + v1._1 + "," + v1._2 + ")-->(" + v2._1 + "," + v2._2 + ")");
                return new Tuple2(v1._1 + v2._1, v1._2 + v2._2);
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
 * $ spark-submit --master yarn --class transform.CombineTest /home/yong/stu-hadoop/spark-pair-rdd/target/spark-pair-rdd-1.0-SNAPSHOT.jar > CombineTest.log
 */