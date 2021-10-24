package vsst;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BracketsCalculator extends Mapper<Object, Text, IntWritable, DoubleWritable> {
    private double getFirst(double a, double b) {
        return a + b;
    }

    private double getSecond(double a, double b) {
        return a - 2 * b;
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        int i = Integer.parseInt(values[0]);
        double a = Double.parseDouble(values[1]);
        double b = Double.parseDouble(values[2]);

        context.write(new IntWritable(i), new DoubleWritable(getFirst(a, b)));
        context.write(new IntWritable(i), new DoubleWritable(getSecond(a, b)));
    }
}
