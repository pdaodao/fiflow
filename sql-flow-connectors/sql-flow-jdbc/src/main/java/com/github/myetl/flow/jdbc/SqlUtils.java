package com.github.myetl.flow.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class SqlUtils {

    public static void print(ResultSet rs) {
        try {
            ResultSetMetaData metaData = rs.getMetaData();

            int count = metaData.getColumnCount();

            for (int i = 1; i <= count; i++) {
                System.out.print(metaData.getColumnName(i));
                System.out.print("  |   ");
            }
            System.out.println();
            System.out.println("----------------------------------------");

            while (rs.next()) {

                for (int i = 1; i <= count; i++) {
                    System.out.print(metaData.getColumnName(i));
                    System.out.print(" : ");
                    System.out.print(rs.getString(i));
                    System.out.print("  |   ");
                }
                System.out.println();
                System.out.println();
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
