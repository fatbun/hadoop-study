package com.benjamin.hadoopstudy.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TopNReduce extends Reducer<TopNKey, IntWritable, Text, IntWritable> {

    Text rkey = new Text();
    IntWritable rval = new IntWritable();

    @Override
    protected void reduce(TopNKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 1970-6-4 33   33
        // 1970-6-4 32   32
        // 1970-6-22 31   31
        // 1970-6-4 22   22

        // 因为 GroupingComparator 已经把 年月相同的 TopNKey 分到一组中处理，
        // 而 values 是通过假迭代器 调用 真迭代器的 next方法，
        // 因此假迭代器的 next方法中会重新赋值 key, values
        // 入参 key会随之改变

        // 取相同年月的 top2 温度最高的行：
        // 1、首位的值
        // 2、下一个年月不等于首位的值

        int first = 0;
        int day = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            IntWritable value = iterator.next();

            if (first == 0) {
                first = 1;
                day = key.getDayOfMonth();

                rkey.set(key.getYear() + "-" + key.getMonth() + " " + key.getTemperature());
                rval.set(key.getTemperature());
                context.write(rkey, rval);
            } else if (key.getDayOfMonth() != day) {
                rkey.set(key.getYear() + "-" + key.getMonth() + " " + key.getTemperature());
                rval.set(key.getTemperature());
                context.write(rkey, rval);

                break;
            }
        }

    }
}
