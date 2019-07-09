import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Test {
    @org.junit.Test
    public void testList() {
        List<Integer> intList = new ArrayList<Integer>();
        intList.add(1);
        intList.add(2);
        intList.add(3);
        Iterator intg1 = intList.iterator();
        while (intg1.hasNext()) {
            intg1.next();
        }
        Iterator intg2 = intList.iterator();
        System.out.println(intg1.hashCode() + "-->" + intg2.hashCode());
        System.out.println(intg1.hasNext() + "-->" + intg2.hasNext());
    }
}
