package com.jstructure.transactq.connection;

import com.jstructure.transactq.lib.IConnectionManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class ConnectionManager implements IConnectionManager {
    private static final String PG_DRIVER_PACKAGE = "org.postgresql.Driver";
    private String dbUrl;
    private String dbUser;
    private String dbPassword;

    ConnectionManager(String dbUrl, String dbUser, String dbPassword) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }

    @Override
    public Connection createConnection() {
        try {
            Class.forName(PG_DRIVER_PACKAGE);
            return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace(); // TODO: use logger
        }
        return null;
    }
}
