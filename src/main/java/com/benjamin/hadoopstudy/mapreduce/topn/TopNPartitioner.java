package com.benjamin.hadoopstudy.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TopNPartitioner extends Partitioner<TopNKey, IntWritable> {

    public int getPartition(TopNKey topNKey, IntWritable intWritable, int numPartitions) {

        // 分区：年月分到同一个 partition

        return topNKey.getYear() % numPartitions;
    }
}
