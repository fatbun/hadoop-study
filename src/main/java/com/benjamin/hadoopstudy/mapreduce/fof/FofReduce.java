package com.benjamin.hadoopstudy.mapreduce.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FofReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable rval = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // key：马老师_一明哥
        // value：0,1,1,0,1
        // 只要发现存在一个直接好友关系（即value为0）则不做write

        // 标记是否存在直接好友关系
        int flag = 0;
        // 标记有多少个间接关系
        int sum = 0;
        Iterator<IntWritable> itr = values.iterator();
        while (itr.hasNext()) {
            IntWritable next = itr.next();
            if (next.get() == 0) {
                flag++;
                break;
            }

            sum += next.get();
        }

        if (flag == 0) {
            rval.set(sum);
            context.write(key, rval);
        }
    }
}
