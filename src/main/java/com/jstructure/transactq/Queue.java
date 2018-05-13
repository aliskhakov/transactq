package com.jstructure.transactq;

import com.jstructure.transactq.exception.ActiveMessageExistsException;
import com.jstructure.transactq.lib.IMessage;
import com.jstructure.transactq.lib.IQueue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class Queue implements IQueue {
    static final String GET_FIRST_MESSAGE_SQL = "SELECT payload, id " +
            "FROM message " +
            "WHERE queue_id = ? " +
            "ORDER BY created_at " +
            "LIMIT 1 " +
            "FOR UPDATE SKIP LOCKED";
    static final String DELETE_MESSAGE_SQL = "DELETE FROM message WHERE id = ?";
    static final String INSERT_MESSAGE_SQL = "INSERT INTO message(queue_id, payload) VALUES (?, ?)";

    static final String MESSAGE_PAYLOAD_COLUMN_KEY = "payload";
    static final String MESSAGE_ID_COLUMN_KEY = "id";

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
    public IMessage get() {
        if (activeMessage != null) {
            throw new ActiveMessageExistsException();
        }

        try (PreparedStatement statement = connection.prepareStatement(GET_FIRST_MESSAGE_SQL)) {
            connection.setAutoCommit(false);
            statement.setLong(1, id);
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    activeMessage = new Message(result.getString(MESSAGE_PAYLOAD_COLUMN_KEY), result.getLong(MESSAGE_ID_COLUMN_KEY));
                } else {
                    connection.commit();
                    connection.setAutoCommit(true);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace(); // TODO: logger
        }
        return activeMessage;
    }

    @Override
    public boolean ack() {
        boolean result = false;
        try (PreparedStatement statement = connection.prepareStatement(DELETE_MESSAGE_SQL)) {
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
        try (PreparedStatement statement = connection.prepareStatement(INSERT_MESSAGE_SQL)) {
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
