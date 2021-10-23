package com.benjamin.hadoopstudy.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcClient {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection conn = DriverManager.getConnection("jdbc:hive2://node04:10000/test", "root", "");
        Statement stmt = conn.createStatement();
        String sql = "select * from psn_external";
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t"
					+ res.getString("name") + "\t"
					+ res.getString("likes") + "\t"
					+ res.getString("address"));
        }
    }
}

