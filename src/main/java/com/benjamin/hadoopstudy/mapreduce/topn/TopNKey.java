package com.benjamin.hadoopstudy.mapreduce.topn;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class TopNKey implements WritableComparable<TopNKey> {

    private Integer year;
    private Integer month;
    private Integer dayOfMonth;
    private Integer temperature;

    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(dayOfMonth);
        out.writeInt(temperature);
    }

    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.dayOfMonth = in.readInt();
        this.temperature = in.readInt();
    }

    /**
     * 通用排序，按日期正序
     *
     * @param that
     * @return
     */
    public int compareTo(TopNKey that) {

        int c1 = Integer.compare(this.year, that.year);
        if (c1 == 0) {
            int c2 = Integer.compare(this.month, that.month);
            if (c2 == 0) {
                return Integer.compare(this.dayOfMonth, that.dayOfMonth);
            }
            return c2;
        }

        return c1;
    }
}
