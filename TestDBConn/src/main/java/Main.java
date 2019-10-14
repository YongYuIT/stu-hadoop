import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class Main {

    //$ java -jar TestDBConn-1.0-SNAPSHOT-jar-with-dependencies.jar

    private static String url = "";
    private static String name = "";
    private static String username = "";
    private static String password = "";
    private static String sql = "";

    private static Connection connection = null;
    private static Statement stmt = null;

    public static void main(String[] args) {
        try {
            Map config = readConfig();
            System.out.println("##$$##-->url-->" + config.get("url"));
            System.out.println("##$$##-->name-->" + config.get("name"));
            System.out.println("##$$##-->username-->" + config.get("username"));
            System.out.println("##$$##-->password-->" + config.get("password"));
            System.out.println("##$$##-->sql-->" + config.get("sql"));
            url = config.get("url").toString();
            name = config.get("name").toString();
            username = config.get("username").toString();
            password = config.get("password").toString();
            sql = config.get("sql").toString();

            Class.forName(name);
            connection = DriverManager.getConnection(url, username, password);
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            rs.next();
            int count = rs.getInt("count_num");
            System.out.println("##$$##-->" + count);
            rs.close();
            stmt.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Map<String, String> readConfig() throws Exception {
        Map result = new HashMap();
        File confFile = new File("config.txt");
        System.out.println("##$$##-->reading from-->" + confFile.getAbsolutePath());
        FileInputStream fileInputStream = new FileInputStream(confFile);
        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String lineTxt = null;
        while ((lineTxt = bufferedReader.readLine()) != null) {
            String[] kv = lineTxt.split("-->");
            result.put(kv[0].trim(), kv[1].trim());
        }
        bufferedReader.close();
        inputStreamReader.close();
        fileInputStream.close();
        return result;
    }

}
