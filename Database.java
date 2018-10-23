package com.db;

import java.sql.SQLException;
import java.util.Properties;

public interface Database {

    public String getDBUrl();

    public String getDBPasswor();

    public String getDBUser();

    public void loadRecords() throws SQLException,ClassNotFoundException;

    public Properties getConnectionProperties();

}
