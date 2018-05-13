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
    static final String UPSERT_QUEUE_SQL = "INSERT INTO queue(name) VALUES (?) ON CONFLICT DO NOTHING";
    static final String DELETE_QUEUE_SQL = "DELETE FROM queue WHERE name = ?";
    static final String GET_QUEUE_SQL = "SELECT id FROM queue WHERE name = ?";

    static final String QUEUE_ID_COLUMN_KEY = "id";

    private Connection connection;
    private Map<String, IQueue> queues = new HashMap<>();

    public Storage(Connection connection) {
        this.connection = connection;
    }

    @Override
    public boolean declareQueue(String name) {
        try (PreparedStatement statement = connection.prepareStatement(UPSERT_QUEUE_SQL)) {
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
        try (PreparedStatement statement = connection.prepareStatement(DELETE_QUEUE_SQL)) {
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
        return queues.computeIfAbsent(name, k -> {
            IQueue queue = queues.get(k);
            try (PreparedStatement statement = connection.prepareStatement(GET_QUEUE_SQL)) {
                statement.setString(1, k);
                try (ResultSet result = statement.executeQuery()) {
                    // TODO: check if queue exists
                    if (result.next()) {
                        queue = new Queue(connection, result.getLong(QUEUE_ID_COLUMN_KEY));
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return queue;
        });
    }
}
