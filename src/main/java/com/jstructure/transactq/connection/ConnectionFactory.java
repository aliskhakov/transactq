package com.jstructure.transactq.connection;

import com.jstructure.transactq.lib.IConnectionManager;

import java.sql.Connection;
import java.util.HashMap;

public class ConnectionFactory {
    private static volatile ConnectionFactory instance;
    private HashMap<String, IConnectionManager> connectionManagers = new HashMap<>();

    private ConnectionFactory() {
    }

    public static ConnectionFactory getInstance() {
        ConnectionFactory instance;
        instance = ConnectionFactory.instance;
        if (instance == null) {
            synchronized (ConnectionFactory.class) {
                instance = ConnectionFactory.instance;
                if (instance == null) {
                    ConnectionFactory.instance = new ConnectionFactory();
                }
            }
        }
        return ConnectionFactory.instance;
    }

    public Connection createConnection(String dbUrl, String dbUser, String dbPassword) {
        IConnectionManager connectionManager = getConnectionManager(dbUrl, dbUser, dbPassword);
        return connectionManager.createConnection();
    }

    private IConnectionManager getConnectionManager(String dbUrl, String dbUser, String dbPassword) {
        if (!connectionManagers.containsKey(dbUrl)) {
            ConnectionManager connectionManager = new ConnectionManager(dbUrl, dbUser, dbPassword);
            connectionManagers.put(dbUrl, connectionManager);
        }
        return connectionManagers.get(dbUrl);
    }
}
