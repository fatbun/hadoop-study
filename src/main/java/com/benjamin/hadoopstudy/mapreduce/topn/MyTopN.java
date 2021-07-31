package com.benjamin.hadoopstudy.mapreduce.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Properties;

public class MyTopN {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name","local");

        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME", "root");

        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Path inputPath = new Path(remainingArgs[0]);
        Path outputPath = new Path(remainingArgs[1]);

        Job job = Job.getInstance(conf, "topn");
        job.setJarByClass(MyTopN.class);

        //maptask
        //input
        TextInputFormat.addInputPath(job, inputPath);
        TextOutputFormat.setOutputPath(job, outputPath);

        //key
        //map
        job.setMapperClass(TopNMapper.class);
        job.setOutputKeyClass(TopNKey.class);
        job.setOutputValueClass(IntWritable.class);

        //partitioner
        job.setPartitionerClass(TopNPartitioner.class);

        //sortComparator
        job.setSortComparatorClass(TopNSortComparator.class);

        //combine
//        job.setCombinerClass(TopNCombiner.class);

        //reducetask
        //groupingComparator
        job.setGroupingComparatorClass(TopNGroupingComparator.class);
        //reduce
        job.setReducerClass(TopNReduce.class);

        job.waitForCompletion(true);
    }
}
