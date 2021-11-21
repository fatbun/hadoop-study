package com.benjamin.hadoopstudy.hbase.mrfromhbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Ben.Li
 * @date 2021/11/21 下午10:11
 */
public class WriteToHdfsReducer extends Reducer<ImmutableBytesWritable, Result, NullWritable, Text> {

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
        for (Result result : values) {
            CellScanner scanner = result.cellScanner();
            while (scanner.advance()) {
                Cell cell = scanner.current();
                Text text = new Text();

                String row = Bytes.toString(CellUtil.cloneRow(cell)) + "\t";
                String cf = Bytes.toString(CellUtil.cloneFamily(cell)) + "\t";
                String cn = Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t";
                String value = Bytes.toString(CellUtil.cloneValue(cell)) + "\t";

                StringBuffer buffer = new StringBuffer();
                buffer.append(row)
                        .append(cf)
                        .append(cn)
                        .append(value);

                text.set(buffer.toString());

                context.write(NullWritable.get(), text);
            }
        }
    }
}
