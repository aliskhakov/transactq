package com.jstructure.transactq;

import com.jstructure.transactq.lib.IQueue;
import com.jstructure.transactq.lib.IStorage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class Storage implements IStorage {
    private Connection connection;

    private Map<String, IQueue> queues = new HashMap<>();

    public Storage(Connection connection) {
        this.connection = connection;
    }

    @Override
    public boolean declareQueue(String name) {
        try {
            PreparedStatement statement = connection.prepareStatement("INSERT INTO queue(name) VALUES (?) " +
                    "ON CONFLICT DO NOTHING");
            statement.setString(1, name);
            statement.executeUpdate();
            // TODO: put queue to local queues
            return true;
        } catch (SQLException e) {
            e.printStackTrace(); // TODO: logger
        }
        return false;
    }

    @Override
    public boolean deleteQueue(String name) {
        try {
            PreparedStatement statement = connection.prepareStatement("DELETE FROM queue WHERE name = ?");
            statement.setString(1, name);
            statement.executeQuery();
            // TODO: check if queue removed
            queues.remove(name);
            return true;
        } catch (SQLException e) {
            e.printStackTrace(); // TODO: logger
        }
        return false;
    }

    @Override
    public IQueue getQueue(String name) {
        IQueue queue = queues.get(name);
        if (queue == null) {
            try {
                PreparedStatement statement = connection.prepareStatement("SELECT id FROM queue WHERE name = ?");
                statement.setString(1, name);
                ResultSet result = statement.executeQuery();
                // TODO: check if queue exists
                if (result.next()) {
                    queue = new Queue(connection, result.getLong("id"));
                }
                queues.put(name, queue);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return queue;
    }
}
