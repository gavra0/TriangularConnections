package ch.epfl.data.bigdata.hw2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Ivan Gavrilović
 */
public class FinalCountReducer extends Reducer<IntWritable, IntWritable, NullWritable, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
        }
        context.write(NullWritable.get(), new IntWritable(sum));
    }
}