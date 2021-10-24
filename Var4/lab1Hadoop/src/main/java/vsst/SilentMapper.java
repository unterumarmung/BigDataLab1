package vsst;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SilentMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private static Text KEY = new Text("");
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        double a = Double.parseDouble(values[1]);

        context.write(KEY, new DoubleWritable(a));
    }
}
