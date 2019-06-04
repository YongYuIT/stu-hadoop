import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.conf.Configuration;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class EnvTest {
    @Test
    public void test() throws Exception {
        String uri = "hdfs://localhost/";
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), config);

        // 列出hdfs上/目录下的所有文件和目录
        FileStatus[] statuses = fs.listStatus(new Path("/"));
        for (FileStatus status : statuses) {
            System.out.println(status);
        }

        try {
            FileStatus status = fs.getFileStatus(new Path("/user/yong"));
            System.out.println("is path-->" + status.isDirectory());
        } catch (Exception e) {
            System.out.println("path not exist");
        }

        // 在hdfs的/user/yong目录下创建一个文件，并写入一行文本
        FSDataOutputStream os = fs.create(new Path("/user/yong/test.log"));
        os.write("Hello World!".getBytes());
        os.flush();
        os.close();

        // 显示在hdfs的/user/yong下指定文件的内容
        InputStream is = fs.open(new Path("/user/yong/test.log"));
        IOUtils.copyBytes(is, System.out, 1024, true);
    }
}
