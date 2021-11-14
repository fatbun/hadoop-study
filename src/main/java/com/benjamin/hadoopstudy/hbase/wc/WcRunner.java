package com.benjamin.hadoopstudy.hbase.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @author Ben.Li
 * @date 2021/11/14 下午3:39
 * <p>
 * MR是分布式计算框架，对于数据源和数据目的地没有限制，用户可以任意选择，只不过需要实现两个类
 * InputFormat
 * -     getSplits()
 * -     createRecordReader()
 * OutputFormat
 * -     getRecordWriter(): 返回值: RecordWriter
 * -                                    write()
 * -                                    close()
 * <p>
 * 注意：
 * -    当需要从hbase读取数据的时候，必须使用TableMapReduceUtil.initTableMapperJob()
 * -    当需要写数据到hbase的时候，必须使用TableMapReduceUtil.initTableReduceJob()
 * -        如果在代码逻辑进行实现的时候，不需要reduce，只要向hbase写数据，那么上面的方法必须存在
 * <p>
 * 实现wordcount
 * 1、从hdfs读取数据
 * 2、将数据的结果存储到hbase
 * <p>
 * 作业：
 * 从hbase读取数据，将结果写入hdfs
 */
public class WcRunner {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        conf.set("hbase.zookeeper.quorum", "node02,node03,node04");
        conf.set("mapreduce.framework.name", "local");

        //创建job对象
        Job job = Job.getInstance(conf);
        job.setJarByClass(WcRunner.class);

        //设置mapper类
        job.setMapperClass(WcMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reducer类
        TableMapReduceUtil.initTableReducerJob("wc",
                WcReducer.class,
                job,
                null,
                null,
                null,
                null,
                false);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        //指定hdfs存储数据的目录
        FileInputFormat.addInputPath(job, new Path("/data/wc/input"));
        job.waitForCompletion(true);
    }
}
