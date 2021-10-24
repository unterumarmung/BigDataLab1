package vsst;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DoubleMultiplyReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double res = 1;
        for (DoubleWritable val : values) {
            res *= val.get();
        }
        result.set(res);
        context.write(key, result);
    }
}
