import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class HelloMR {


    public static class MyMap extends Mapper<Object, Text, IntWritable, FloatWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("map key-->" + key + "-->" + value.toString());
            String[] year_tmp = value.toString().split("-->");
            context.write(new IntWritable(Integer.parseInt(year_tmp[0])), new FloatWritable(Float.parseFloat(year_tmp[1])));
        }
    }

    public static class MyReduce extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float max_tmp = 0;
            for (FloatWritable value : values) {
                if (value.get() > max_tmp) {
                    max_tmp = value.get();
                }
            }
            context.write(key, new FloatWritable(max_tmp));
        }
    }


    /*
     * $ hdfs dfs -rm -r /user/yong/out
     * $ hadoop jar /home/yong/Desktop/hello_map_reduce/target/hello_map_reduce-1.0-SNAPSHOT.jar HelloMR /user/yong/input/test_data /user/yong/out
     * if Error: Could not find or load main class org.apache.hadoop.mapreduce.v2.app.MRAppMaster
     * $ hadoop classpath
     * /mnt/hgfs/hadoop-env/hadoop-3.1.2/my_config:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/common/lib/*:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/common/*:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/hdfs:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/hdfs/lib/*:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/hdfs/*:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/mapreduce/lib/*:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/mapreduce/*:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/yarn:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/yarn/lib/*:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/yarn/*
     * $ gedit $HADOOP_CONF_DIR/yarn-site.xml
       <property>
         <name>yarn.application.classpath</name>
         <value>/mnt/hgfs/hadoop-env/hadoop-3.1.2/my_config:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/common/lib/*:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/common/*:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/hdfs:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/hdfs/lib/*:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/hdfs/*:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/mapreduce/lib/*:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/mapreduce/*:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/yarn:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/yarn/lib/*:/mnt/hgfs/hadoop-env/hadoop-3.1.2/share/hadoop/yarn/*</value>
       </property>
     * if container_1559636300279_0004_01_000008] is running 364583424B beyond the 'VIRTUAL' memory limit.
     * $ gedit $HADOOP_CONF_DIR/mapred-site.xml
     * <property>
     *   <name>mapreduce.map.memory.mb</name>
     *   <value>1536</value>
     * </property>
     * <property>
     *   <name>mapreduce.map.java.opts</name>
     *   <value>-Xmx1024M</value>
     * </property>
     * <property>
     *   <name>mapreduce.reduce.memory.mb</name>
     *   <value>3072</value>
     * </property>
     * <property>
     *   <name>mapreduce.reduce.java.opts</name>
     *   <value>-Xmx2560M</value>
     * </property>
     * $ hdfs dfs -ls /user/yong/out
     * $ hdfs dfs -cat /user/yong/out/part-r-00000
     * $ cd $HADOOP_HOME/logs/userlogs/
     * applicationXXX/containerXXX/stdout
     */

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: EventCount <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "my HelloMR");
        job.setJarByClass(HelloMR.class);
        job.setMapperClass(MyMap.class);
        job.setCombinerClass(MyReduce.class);
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

