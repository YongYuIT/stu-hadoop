import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Main {

    //$ java -jar TestDBConn-1.0-SNAPSHOT-jar-with-dependencies.jar

    private static final String url = "jdbc:mysql://30.23.9.255:3306/mlsc_xinfang_shenzhendb";
    private static final String name = "com.mysql.cj.jdbc.Driver";
    private static final String username = "dw_developer";
    private static final String password = "dw_devRs#0";

    private static Connection connection = null;
    private static Statement stmt = null;

    public static void main(String[] args) {
        try {
            Class.forName(name);
            connection = DriverManager.getConnection(url, username, password);
            stmt = connection.createStatement();
            String sql = "select count(*) as count_num from xf_complain";
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

}
