package action_test;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TakeTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("action_test.TakeTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List input = Arrays.asList(new Tuple2(1, "A1"), new Tuple2(2, "A2"), new Tuple2(3, "A3"), new Tuple2(4, "B1"), new Tuple2(5, "B2"), new Tuple2(6, "B3"));
        JavaPairRDD<Integer, String> pairRdd = sc.parallelizePairs(input);
        System.out.println("pairRdd Partition before-->" + pairRdd.partitioner().isPresent());
        List take_before = pairRdd.take(3);
        printPair(take_before);

        pairRdd = pairRdd.partitionBy(new Partitioner() {
            @Override
            public int numPartitions() {
                return 2;
            }

            @Override
            public int getPartition(Object key) {
                if (((Integer) key) < 4)
                    return 0;
                else return 1;
            }
        });
        System.out.println("pairRdd Partition after-->" + pairRdd.partitioner().isPresent());
        List take_after = pairRdd.take(3);
        printPair(take_after);

    }

    private static void printPair(List<Tuple2> tuple2s) {
        for (Tuple2 tuple2 : tuple2s) {
            System.out.print(tuple2._1() + "-->" + tuple2._2 + "    ");
        }
        System.out.println();
    }
}

/*
 * $ spark-submit --class action_test.TakeTest /home/yong/stu-hadoop20190725001/spark-rdd/target/spark-rdd-1.0-SNAPSHOT.jar
 *
 */
