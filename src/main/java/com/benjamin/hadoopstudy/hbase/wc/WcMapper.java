package com.benjamin.hadoopstudy.hbase.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Ben.Li
 * @date 2021/11/14 下午4:21
 */
public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split(" ");
        for (String split : splits) {
            context.write(new Text(split), new IntWritable(1));

        }
    }
}
