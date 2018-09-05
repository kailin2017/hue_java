package com.cloudera.impala_example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class Impala {

    private static final String SQL_STATEMENT = "select * from tokenized_access_logs limit 2000";
    private static final String IMPALA_HOST = "35.234.23.58";
    private static final String IMPALA_PORT = "21050";
    private static final String CONNECTION_URL = String.format("jdbc:hive2://%s:%s/default;auth=noSasl", IMPALA_HOST, IMPALA_PORT);

    public static void main(String[] args) {
        System.out.println(String.format("Connection to %s", IMPALA_HOST));
        System.out.println(String.format("Running Query %s", SQL_STATEMENT));
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Connection connection = DriverManager.getConnection(CONNECTION_URL);
            ResultSet resultSet = connection.createStatement().executeQuery(SQL_STATEMENT);
            int i = 1;
            while (resultSet.next()) {
                System.out.println(String.format("No.%d : %s %s", i, resultSet.getString(1), resultSet.getString(2)));
                i++;
            }
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

}
