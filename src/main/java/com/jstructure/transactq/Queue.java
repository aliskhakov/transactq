package com.jstructure.transactq;

import com.jstructure.transactq.lib.IMessage;
import com.jstructure.transactq.lib.IQueue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class Queue implements IQueue {
    private Connection connection;

    private long id;

    private Message activeMessage;

    Queue(Connection connection, long id) {
        this.connection = connection;
        this.id = id;
    }

    @Override
    public List<IMessage> get(int limit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IMessage get() throws Exception {
        if (activeMessage != null) {
            throw new Exception("You have not acked message. Ack it before.");
        }

        try {
            connection.setAutoCommit(false);
            PreparedStatement statement = connection.prepareStatement("SELECT payload, id " +
                    "FROM message " +
                    "WHERE queue_id = ? " +
                    "ORDER BY created_at " +
                    "LIMIT 1 " +
                    "FOR UPDATE SKIP LOCKED ");
            statement.setLong(1, id);
            ResultSet result = statement.executeQuery();
            if (result.next()) {
                activeMessage = new Message(result.getString("payload"), result.getLong("id"));
            } else {
                connection.commit();
                connection.setAutoCommit(true);
            }
            return activeMessage;
        } catch (SQLException e) {
            e.printStackTrace(); // TODO: logger
        }
        return null;
    }

    @Override
    public boolean ack() {
        boolean result = false;
        try {
            PreparedStatement statement = connection.prepareStatement("DELETE FROM message WHERE id = ?");
            statement.setLong(1, activeMessage.getId());
            statement.executeUpdate();
            connection.commit();
            // TODO: check if message deleted
            activeMessage = null;
            result = true;
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace(); // TODO: logger
            }
            e.printStackTrace(); // TODO: logger
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();  // TODO: logger, throw domain exception
            }
        }
        return result;
    }

    @Override
    public boolean push(IMessage message) {
        try {
            PreparedStatement statement = connection.prepareStatement("INSERT INTO message(queue_id, payload) VALUES (?, ?)");
            statement.setLong(1, id);
            statement.setString(2, message.getPayload());
            statement.executeUpdate();
            // TODO: check if message created
            return true;
        } catch (SQLException e) {
            e.printStackTrace(); // TODO: logger
        }
        return false;
    }
}
