package com.belo82.hdpwordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Hello world!
 */
public class App {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

//        Job job = new Job(conf, "dictionary");
        Job job = Job.getInstance();// new Job(conf, "dictionary");
        job.setJobName("dictionary");

        job.setJarByClass(App.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(AllTranslationsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date())));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
