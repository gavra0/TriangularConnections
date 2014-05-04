package ch.epfl.data.bigdata.hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Run extends Configured implements Tool {

    static final int NUM_NODES = 88;
    static final String TMP_PATH = "/std090/tmp";

    @Override
    public int run(String[] args) throws Exception {
        Configuration jobConf = new Configuration();

        Job job1 = new Job(jobConf, "triangular_search");


        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntArrayWritable.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setJarByClass(Run.class);
        job1.setMapperClass(TriangularConnectionMapper.class);
        job1.setReducerClass(TriangularConnectionsReducer.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setNumReduceTasks(NUM_NODES);

        FileSystem fs = FileSystem.get(jobConf);
        fs.delete(new Path(TMP_PATH), true);

        long chunkSize = 2 * (fs.getFileStatus(new Path(args[0])).getLen() / NUM_NODES);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileInputFormat.setMaxInputSplitSize(job1, chunkSize);
        FileInputFormat.setMinInputSplitSize(job1, chunkSize);
        FileOutputFormat.setOutputPath(job1, new Path(TMP_PATH));

        Job job2 = new Job(jobConf);
        job2.setJobName("final_count");

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);

        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(LongWritable.class);

        job2.setJarByClass(Run.class);
        job2.setMapperClass(FinalCountMapper.class);
        job2.setReducerClass(FinalCountReducer.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job2, new Path(TMP_PATH));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        job1.waitForCompletion(true);
        job2.waitForCompletion(true);
        return 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Run(), args);
        System.exit(res);
    }
}	
