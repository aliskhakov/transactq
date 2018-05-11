package com.jstructure.transactq.connection;

import com.jstructure.transactq.lib.IConnectionManager;

import java.util.HashMap;

public class ConnectionManagerFactory {
    private HashMap<String, IConnectionManager> connectionManagers = new HashMap<>();
    private static ConnectionManagerFactory instance;

    private ConnectionManagerFactory() {

    }

    public static ConnectionManagerFactory getInstance() {
        if (instance == null) {
            instance = new ConnectionManagerFactory();
        }
        return instance;
    }

    public IConnectionManager createConnectionManager(String dbUrl, String dbUser, String dbPassword) {
        if (!connectionManagers.containsKey(dbUrl)) {
            ConnectionManager connectionManager = new ConnectionManager(dbUrl, dbUser, dbPassword);
            connectionManagers.put(dbUrl, connectionManager);
        }
        return connectionManagers.get(dbUrl);
    }
}
