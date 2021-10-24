package vsst;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

class WordCountDescriber extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordCount = value.toString().split("\t");
        String word = wordCount[0];
        int count = Integer.parseInt(wordCount[1]);

        String result;

        if (count <= 10) {
            result = "Очень редко";
        } else if (count <= 50) {
            result = "Редко";
        } else if (count <= 200) {
            result = "Часто";
        } else {
            result = "Очень часто";
        }

        context.write(new Text(word), new Text(result));
    }
}