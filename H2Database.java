package com.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class H2Database implements Database {

    private String JDBC_URL = "jdbc:h2:file:/Users/d4109611/project/software/data";
    private String USER="sa";
    private String PASSWORD="sa";

    @Override
    public Properties getConnectionProperties() {
        Properties connectionProperties=new Properties();
        connectionProperties.put("user", "sa");
        connectionProperties.put("password", "sa");
        connectionProperties.put("fetchsize", "10000") ;//adjust to suit database resources and table size
        connectionProperties.put("numPartitions", "200"); //adjust to suit database resources and table size
        connectionProperties.setProperty("Driver", "org.h2.Driver");


        return connectionProperties;
    }

    @Override
    public void loadRecords() throws ClassNotFoundException, SQLException {
        Class.forName("org.h2.Driver"); //make sure oracle driver is on the server and included in class path

        //STEP 3: Open a connection
        System.out.println("Connecting to a selected database...");
        Connection connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        System.out.println("Connected database successfully...");

        //STEP 4: Execute a query
        System.out.println("Inserting records into the table...");
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS ORDERS");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ORDERS (ORDERNUMBER INT,ORDERDATE VARCHAR ,REQUIREDDATE VARCHAR,SHIPPEDDATE VARCHAR,STATUS VARCHAR(100),CUSTOMERNUMBER VARCHAR(100))");
        stmt.executeUpdate("INSERT INTO ORDERS VALUES(10418,'20170619','20170626','20170624','Shipped','412')");

        System.out.println("Inserted records into the table...");
    }

    @Override
    public String getDBUrl() {
        return JDBC_URL;
    }

    @Override
    public String getDBPasswor() {
        return PASSWORD;
    }

    @Override
    public String getDBUser() {
        return USER;
    }
}
