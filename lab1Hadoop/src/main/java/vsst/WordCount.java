package vsst;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("usage: " + getClass().getSimpleName() + " [generic options] <inputfile> <outputdir> <outputdir2>");
            System.exit(1);
        }
        Configuration conf = this.getConf();
        Job wordCountJob = Job.getInstance(conf, "Word Count");
        wordCountJob.setJarByClass(WordCount.class);
        wordCountJob.setMapperClass(TokenizerMapper.class);
        wordCountJob.setCombinerClass(IntSumReducer.class);
        wordCountJob.setReducerClass(IntSumReducer.class);
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));

        if (!wordCountJob.waitForCompletion(true))
            return 1;

        conf = this.getConf();
        Job wordCountDescriberJob = Job.getInstance(conf, "Word Count Describer");
        wordCountDescriberJob.setJarByClass(WordCount.class);
        wordCountDescriberJob.setMapperClass(WordCountDescriber.class);
        wordCountDescriberJob.setNumReduceTasks(0);
        wordCountDescriberJob.setOutputKeyClass(Text.class);
        wordCountDescriberJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(wordCountDescriberJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(wordCountDescriberJob, new Path(args[2]));

        if (!wordCountDescriberJob.waitForCompletion(true))
            return 1;

        return 0;
    }
}
