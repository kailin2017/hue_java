package com.cloudera.impala_example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Impala {

    private static final String SQL_STATEMENT = "select * from tokenized_access_logs limit 2000";
    private static final String IMPALA_HOST = "35.239.91.94";
    private static final String IMPALA_PORT = "21050";
    private static final String CONNECTION_URL = String.format("jdbc:hive2://%s:%s/default;auth=noSasl", IMPALA_HOST, IMPALA_PORT);

    public static void main(String[] args) {
        System.out.println(String.format("Connection to %s", IMPALA_HOST));
        System.out.println(String.format("Running Query %s", SQL_STATEMENT));
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Connection connection = DriverManager.getConnection(CONNECTION_URL);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(SQL_STATEMENT);
            while (resultSet.next()) {
                String s = String.format("No.%04d | %15s | %3s | %s",
                        resultSet.getRow(),
                        resultSet.getString(2),
                        resultSet.getString(6),
                        resultSet.getString(4));
                System.out.println(s);
            }
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

}