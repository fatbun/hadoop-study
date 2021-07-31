package com.benjamin.hadoopstudy.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class TopNMapper extends Mapper<Object, Text, TopNKey, IntWritable> {

    // 因为map可能被吊起多次，定义在外边减少gc，同时，你要知道，源码中看到了，
    // map输出的key，value，是会发生序列化，变成字节数组进入buffer的
    private TopNKey tKey = new TopNKey();
    private IntWritable tValue = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // value:  2019-6-1 22:22:22	1	31

        // 解析value
        List<String> tokens = new ArrayList<String>();
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            tokens.add(token);
        }

        String date = tokens.get(0);
        Integer temperature = Integer.valueOf(tokens.get(3));

        DateTimeFormatter dtf= DateTimeFormat.forPattern("yyyy-MM-dd");
        DateTime datetime = dtf.parseDateTime(date);

        tKey.setYear(datetime.getYear());
        tKey.setMonth(datetime.getMonthOfYear());
        tKey.setDayOfMonth(datetime.getDayOfMonth());
        tKey.setTemperature(temperature);

        tValue.set(temperature);

        context.write(tKey, tValue);
    }

    public static void main(String[] args) {
        String str = "2019-6-1 22:22:22	1	31";
        StringTokenizer itr = new StringTokenizer(str);
        List<String> tokens = new ArrayList<String>();

        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();

            tokens.add(token);
        }

        System.out.println(tokens);
    }
}
