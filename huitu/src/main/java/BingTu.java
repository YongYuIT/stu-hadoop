import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

import javax.swing.*;
import java.awt.*;

public class BingTu {
    public static void main(String[] args) {
        JFrame frame = new JFrame("bigTitle");
        frame.setLayout(new GridLayout(2, 2, 10, 10));
        frame.add(getChart());
        frame.setBounds(50, 50, 800, 600);
        frame.setVisible(true);
    }

    public static ChartPanel getChart() {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        dataset.addValue(100, "AA", "11");
        dataset.addValue(200, "BB", "11");
        dataset.addValue(100, "CC", "11");
        dataset.addValue(200, "DD", "11");
        dataset.addValue(100, "AA", "22");
        dataset.addValue(200, "BB", "22");
        dataset.addValue(100, "CC", "22");
        dataset.addValue(200, "DD", "22");
        dataset.addValue(100, "AA", "33");
        dataset.addValue(200, "BB", "33");
        dataset.addValue(100, "CC", "33");
        dataset.addValue(200, "DD", "33");
        JFreeChart chart = ChartFactory.createBarChart("Title", "X-datas", "Y-datas", dataset, PlotOrientation.VERTICAL, true, false, false);
        return new ChartPanel(chart);
    }
}
