package ch.epfl.data.bigdata.hw2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author Ivan Gavrilović
 */
public class FinalCountMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer textTokenizer = new StringTokenizer(value.toString(), "\n");
        // this will read line by line
        while (textTokenizer.hasMoreTokens()) {
            String line = textTokenizer.nextToken();

            String[] values = line.split("\\s");
            if (values.length == 2) {
                context.write(new IntWritable(0), new IntWritable(new Integer(values[1])));
            }
        }
    }
}