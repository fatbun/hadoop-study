package com.benjamin.hadoopstudy.mapreduce.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

public class FofMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text fkey = new Text();
    private IntWritable fval = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 解析 value：马老师 一明哥 连老师 周老师
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        List<String> list = Collections.list(tokenizer).stream()
                .map(token -> (String) token)
                .collect(Collectors.toList());

        // 两个集合 {0}, {1}
        // 0：直接好友
        // 1：间接好友

        for (int i = 1; i < list.size(); i++) {
            // 直接好友
            fkey.set(this.sort(list.get(0), list.get(i)));
            fval.set(0);
            context.write(fkey, fval);

            for (int j = i + 1; j < list.size(); j++) {
                // 间接好友
                fkey.set(this.sort(list.get(i), list.get(j)));
                fval.set(1);
                context.write(fkey, fval);
            }
        }

    }

    private String sort(String name1, String name2) {
        if (name1.compareTo(name2) > 0) {
            return name1 + "_" + name2;
        }

        return name2 + "_" + name1;
    }
}
