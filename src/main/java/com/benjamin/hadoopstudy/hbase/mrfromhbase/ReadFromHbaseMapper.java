package com.benjamin.hadoopstudy.hbase.mrfromhbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.io.IOException;

/**
 * @author Ben.Li
 * @date 2021/11/21 下午10:10
 */
public class ReadFromHbaseMapper extends TableMapper<ImmutableBytesWritable, Result> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
