package com.benjamin.hadoopstudy.hbase.mrfromhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Properties;

/**
 * @author Ben.Li
 * @date 2021/11/21 下午10:19
 */
public class MyRunner {

    public static void main(String[] args) throws Exception {
        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration(true);
        conf.set("hbase.zookeeper.quorum", "node02,node03,node04");
        conf.set("mapreduce.framework.name", "local");

        //创建job对象
        Job job = Job.getInstance(conf);
        job.setJarByClass(MyRunner.class);

        //设置mapper类
        Scan scan = new Scan();
        scan.setCaching(300);
        TableMapReduceUtil.initTableMapperJob("phone",
                scan,
                ReadFromHbaseMapper.class,
                ImmutableBytesWritable.class,
                Result.class,
                job);

        //设置reducer类
        job.setReducerClass(WriteToHdfsReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //指定hdfs存储数据的目录
        FileOutputFormat.setOutputPath(job,new Path("/hdfs/phone"));

        job.waitForCompletion(true);
    }
}
