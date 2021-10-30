package com.benjamin.hadoopstudy.hbase;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author Ben.Li
 * @date 2021/10/23
 */
public class HBaseDemo {
    private static final String TABLE_NAME = "phone";

    private Configuration conf;
    private Connection conn;
    private Admin admin;
    private Table table;
    private TableName tableName = TableName.valueOf(TABLE_NAME);

    @Before
    public void before() throws Exception {
        // 创建配置文件对象
        conf = HBaseConfiguration.create();
        // 加载zookeeper配置
        conf.set("hbase.zookeeper.quorum", "node02,node03,node04");
        // 获取连接
        conn = ConnectionFactory.createConnection(conf);
        // 获取对象
        admin = conn.getAdmin();
        // 获取数据库操作对象
        table = conn.getTable(tableName);
    }

    @Test
    public void createTable() throws IOException {
        // 定义表描述对象
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        // 定义列族描述对象
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf"));
        // 添加列族信息给表
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());

        if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
            // 禁用表
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        // 创建表
        admin.createTable(tableDescriptorBuilder.build());
    }

    @Test
    public void insert() throws Exception {
        Put put = new Put(Bytes.toBytes("1"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes("19"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("gender"), Bytes.toBytes("male"));

        table.put(put);
    }

    @Test
    public void get() throws IOException {
        Get get = new Get(Bytes.toBytes("1"));
        get.addFamily(Bytes.toBytes("cf"));

        Result result = table.get(get);
        Cell cell1 = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("name"));
        Cell cell2 = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("age"));
        Cell cell3 = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("gender"));

        String name1 = Bytes.toString(CellUtil.cloneValue(cell1));
        String name2 = Bytes.toString(CellUtil.cloneValue(cell2));
        String name3 = Bytes.toString(CellUtil.cloneValue(cell3));
        System.out.println(name1);
        System.out.println(name2);
        System.out.println(name3);
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    @Test
    public void insertBatch() throws Exception {
        List<Put> puts = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String phone1 = this.getNumber("158");
            for (int j = 0; j < 1000; j++) {
                String phone2 = this.getNumber("177");
                String duration = String.valueOf(random.nextInt(100));
                String date = this.getDate("2021");
                // 0-phone1主叫，1-phone2主叫
                String type = String.valueOf(random.nextInt(2));

                // rowkey
                String rowkey = phone1 + "_" + (Long.MAX_VALUE - sdf.parse(date).getTime());
                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes("call"), Bytes.toBytes("phone1"), Bytes.toBytes(phone1));
                put.addColumn(Bytes.toBytes("call"), Bytes.toBytes("phone2"), Bytes.toBytes(phone2));
                put.addColumn(Bytes.toBytes("call"), Bytes.toBytes("duration"), Bytes.toBytes(duration));
                put.addColumn(Bytes.toBytes("call"), Bytes.toBytes("date"), Bytes.toBytes(date));
                put.addColumn(Bytes.toBytes("call"), Bytes.toBytes("type"), Bytes.toBytes(type));

                puts.add(put);
            }
        }
        table.put(puts);
    }

    @Test
    public void scan() throws Exception {
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);

        this.printFamilyCall(scanner);
    }

    /**
     * 查询某一个用户3月份的通话记录
     *
     * @throws Exception
     */
    @Test
    public void scanByCondition() throws Exception {
        Scan scan = new Scan();
        String startRow = "15818082913_" + (Long.MAX_VALUE - sdf.parse("20210331000000").getTime());
        String endRow = "15818082913_" + (Long.MAX_VALUE - sdf.parse("20210301000000").getTime());
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(endRow));

        ResultScanner scanner = table.getScanner(scan);
        this.printFamilyCall(scanner);

    }

    /**
     * 查询某个用户所有的主叫电话
     */
    @Test
    public void getType() throws Exception {
        Scan scan = new Scan();
        // 创建过滤器集合
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        // 创建过滤器
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("call"), Bytes.toBytes("type"), CompareOperator.EQUAL, Bytes.toBytes("0"));
        filters.addFilter(filter1);
        // 前缀过滤器
        PrefixFilter filter2 = new PrefixFilter(Bytes.toBytes("15818082913_"));
        filters.addFilter(filter2);
        scan.setFilter(filters);

        ResultScanner scanner = table.getScanner(scan);
        this.printFamilyCall(scanner);

    }

    private void printFamilyCall(ResultScanner scanner) {
        scanner.forEach(result -> {
            Cell cell1 = result.getColumnLatestCell(Bytes.toBytes("call"), Bytes.toBytes("phone1"));
            Cell cell2 = result.getColumnLatestCell(Bytes.toBytes("call"), Bytes.toBytes("phone2"));
            Cell cell3 = result.getColumnLatestCell(Bytes.toBytes("call"), Bytes.toBytes("duration"));
            Cell cell4 = result.getColumnLatestCell(Bytes.toBytes("call"), Bytes.toBytes("date"));
            Cell cell5 = result.getColumnLatestCell(Bytes.toBytes("call"), Bytes.toBytes("type"));

            String phone1 = Bytes.toString(CellUtil.cloneValue(cell1));
            String phone2 = Bytes.toString(CellUtil.cloneValue(cell2));
            String duration = Bytes.toString(CellUtil.cloneValue(cell3));
            String date = Bytes.toString(CellUtil.cloneValue(cell4));
            String type = Bytes.toString(CellUtil.cloneValue(cell5));

            List<String> list = ImmutableList.of(
                    phone1,
                    phone2,
                    duration,
                    date,
                    type
            );

            System.out.println(StringUtils.join(list, ","));
        });
    }


    Random random = new Random();

    private String getNumber(String str) {
        return str + String.format("%08d", random.nextInt(99999999));
    }

    /**
     * @param year
     * @return yyyyMMddHHmmss
     */
    private String getDate(String year) {
        return year + String.format("%02d%02d%02d%02d%02d",
                random.nextInt(12) + 1,
                random.nextInt(31),
                random.nextInt(24),
                random.nextInt(60),
                random.nextInt(60));
    }

    @After
    public void destroy() {
        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
