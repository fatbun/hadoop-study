package com.benjamin.hadoopstudy.mapreduce.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Properties;

public class MyFof {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        conf.set("mapreduce.framework.name","local");

        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME", "root");

        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Path inputPath = new Path(remainingArgs[0]);
        Path outputPath = new Path(remainingArgs[1]);

        Job job = Job.getInstance(conf, "fof");
//        job.setJarByClass(MyFof.class);
        job.setJar("/Users/lb/work/development/hadoop-study/target/hadoop-study-1.0-SNAPSHOT.jar");

        TextInputFormat.addInputPath(job, inputPath);
        TextOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(FofMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setReducerClass(FofReduce.class);

        job.waitForCompletion(true);
    }
}
