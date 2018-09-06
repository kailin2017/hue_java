package com.cloudera.impala_example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Hive {

    private static final String SQL_STATEMENT = "select * from products";
    private static final String IMPALA_HOST = "35.239.91.94";
    private static final String IMPALA_PORT = "10000";
    private static final String CONNECTION_URL = String.format("jdbc:hive2://%s:%s/default", IMPALA_HOST, IMPALA_PORT);
    private static final String USER_NAME = "cloudera";
    private static final String USER_PWD = "cloudera";

    public static void main(String[] args) {

        System.out.println(String.format("Connection to %s", IMPALA_HOST));
        System.out.println(String.format("Running Query %s", SQL_STATEMENT));
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Connection connection = DriverManager.getConnection(CONNECTION_URL, USER_NAME, USER_PWD);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(SQL_STATEMENT);
            while (resultSet.next()) {
                System.out.println(String.format("No.%04d | $%12f | %s", resultSet.getRow(), resultSet.getDouble(5), resultSet.getString(3)));
            }
        } catch (Exception e) {
            System.out.println(e.toString());
        }

    }
}
