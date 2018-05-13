package com.jstructure.transactq;

import com.jstructure.transactq.lib.IQueue;
import com.jstructure.transactq.lib.IStorage;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static junit.framework.TestCase.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageTest {
    private static final String TEST_QUEUE_NAME = "test_queue";
    private static final String QUEUES_FIELD = "queues";
    private static final long TEST_QUEUE_ID = 1L;

    private Connection connection;
    private PreparedStatement preparedStatement;
    private IStorage storage;

    @Before
    public void setUp() {
        connection = mock(Connection.class);
        preparedStatement = mock(PreparedStatement.class);
        storage = new Storage(connection);
    }

    @Test
    public void declareQueueTest() throws SQLException {
        when(connection.prepareStatement(Storage.UPSERT_QUEUE_SQL)).thenReturn(preparedStatement);
        boolean result = storage.declareQueue(TEST_QUEUE_NAME);
        assertTrue(result);
        // TODO: test that queue puts to local queues container
    }

    @Test
    public void declareQueueFailsTest() throws SQLException {
        when(connection.prepareStatement(Storage.UPSERT_QUEUE_SQL)).thenThrow(SQLException.class);
        boolean result = storage.declareQueue(TEST_QUEUE_NAME);
        assertFalse(result);
        // TODO: test that queue not puts to local queues container
    }

    @Test
    public void deleteQueueTest() throws SQLException, NoSuchFieldException, IllegalAccessException {
        Class<?> c = storage.getClass();
        Field field = c.getDeclaredField(QUEUES_FIELD);
        field.setAccessible(true);
        Map<String, IQueue> queues = (Map<String, IQueue>) field.get(storage);
        queues.put(TEST_QUEUE_NAME, new Queue(connection, TEST_QUEUE_ID));

        when(connection.prepareStatement(Storage.DELETE_QUEUE_SQL)).thenReturn(preparedStatement);

        boolean result = storage.deleteQueue(TEST_QUEUE_NAME);
        assertTrue(result);
        assertFalse(queues.containsKey(TEST_QUEUE_NAME));
    }

    @Test
    public void deleteQueueFailsTest() throws SQLException, NoSuchFieldException, IllegalAccessException {
        Class<?> c = storage.getClass();
        Field field = c.getDeclaredField(QUEUES_FIELD);
        field.setAccessible(true);
        Map<String, IQueue> queues = (Map<String, IQueue>) field.get(storage);
        queues.put(TEST_QUEUE_NAME, new Queue(connection, TEST_QUEUE_ID));

        when(connection.prepareStatement(Storage.DELETE_QUEUE_SQL)).thenThrow(SQLException.class);

        boolean result = storage.deleteQueue(TEST_QUEUE_NAME);
        assertFalse(result);
        assertTrue(queues.containsKey(TEST_QUEUE_NAME));
    }

    @Test
    public void getQueueTest() throws SQLException, NoSuchFieldException, IllegalAccessException {
        Class<?> c = storage.getClass();
        Field field = c.getDeclaredField(QUEUES_FIELD);
        field.setAccessible(true);
        Map<String, IQueue> queues = (Map<String, IQueue>) field.get(storage);
        assertFalse(queues.containsKey(TEST_QUEUE_NAME));

        when(connection.prepareStatement(Storage.GET_QUEUE_SQL)).thenReturn(preparedStatement);
        ResultSet resultSet = mock(ResultSet.class);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong(Storage.QUEUE_ID_COLUMN_KEY)).thenReturn(TEST_QUEUE_ID);

        IQueue queue = storage.getQueue(TEST_QUEUE_NAME);
        assertTrue(queues.containsKey(TEST_QUEUE_NAME));
        assertNotNull(queue);
    }

    @Test
    public void getQueueFailsTest() throws SQLException, NoSuchFieldException, IllegalAccessException {
        Class<?> c = storage.getClass();
        Field field = c.getDeclaredField(QUEUES_FIELD);
        field.setAccessible(true);
        Map<String, IQueue> queues = (Map<String, IQueue>) field.get(storage);
        assertFalse(queues.containsKey(TEST_QUEUE_NAME));

        when(connection.prepareStatement(Storage.GET_QUEUE_SQL)).thenThrow(SQLException.class);

        IQueue queue = storage.getQueue(TEST_QUEUE_NAME);
        assertNull(queue);
        assertFalse(queues.containsKey(TEST_QUEUE_NAME));
    }
}
