package com.datademi;

/**
 * Created by genel on 5/13/17.
 */
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Kullanım: Problem 1 <girdi> <çıktı>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(Driver.class);
        job.setJobName("Problem_1");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ProblemMapper.class);
        job.setReducerClass(ProblemReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
