package vsst;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class FormulaCalculator extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new FormulaCalculator(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: " + getClass().getSimpleName() + " [generic options] <inputfile> <outputdir>");
            System.exit(1);
        }
        Configuration conf = this.getConf();
        Job job1 = Job.getInstance(conf, "Formula Calculator 1");
        job1.setJarByClass(FormulaCalculator.class);
        job1.setMapperClass(BracketsCalculator.class);
        job1.setReducerClass(DoubleMultiplyReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/first"));

        if (!job1.waitForCompletion(true))
            return 1;

        Job job2 = Job.getInstance(conf, "Formula Calculator 2");
        job2.setJarByClass(FormulaCalculator.class);
        job2.setMapperClass(SilentMapper.class);
        job2.setReducerClass(DoubleSumReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1] + "/first"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/second"));

        if (!job2.waitForCompletion(true))
            return 1;

        return 0;
    }
}
