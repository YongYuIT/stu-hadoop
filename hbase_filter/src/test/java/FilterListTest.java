import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class FilterListTest {
    @Test
    public void putTestData() throws Exception {
        new SimpleFilterTest().putTestData();
    }

    @Test
    public void filterListTest() throws Exception {

        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("conn success!");

        TableName tableName = TableName.valueOf("student");
        Table table = connection.getTable(tableName);

        FilterList list = new FilterList();
        Filter familyFilter = new FamilyFilter(CompareOperator.EQUAL, new SubstringComparator("basic_info"));
        Filter colFilter = new QualifierFilter(CompareOperator.EQUAL, new SubstringComparator("name"));
        Filter valueFilter = new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("B"));
        list.addFilter(familyFilter);
        list.addFilter(colFilter);
        list.addFilter(valueFilter);

        Scan scan = new Scan();
        scan.setFilter(list);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("name"))));
        }
        scanner.close();
        table.close();
        connection.close();
    }
}
