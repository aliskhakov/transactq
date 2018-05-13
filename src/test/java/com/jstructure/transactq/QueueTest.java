package com.jstructure.transactq;

import com.jstructure.transactq.exception.ActiveMessageExistsException;
import com.jstructure.transactq.lib.IMessage;
import com.jstructure.transactq.lib.IQueue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static junit.framework.TestCase.*;
import static org.mockito.Mockito.*;

public class QueueTest {
    private static final long TEST_QUEUE_ID = 1L;
    private static final long TEST_MESSAGE_ID = 1L;
    private static final String TEST_MESSAGE_PAYLOAD = "some payload";
    private static final String QUEUE_ACTIVE_MESSAGE_FILED = "activeMessage";

    private Connection connection;

    private IQueue queue;

    private PreparedStatement preparedStatement;

    @Before
    public void setUp() {
        connection = mock(Connection.class);
        preparedStatement = mock(PreparedStatement.class);
        queue = new Queue(connection, TEST_QUEUE_ID);
    }

    @Test
    public void getWhenMessageExistsTest() throws Exception {
        when(connection.prepareStatement(Queue.GET_FIRST_MESSAGE_SQL)).thenReturn(preparedStatement);
        ResultSet resultSet = mock(ResultSet.class);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString(Queue.MESSAGE_PAYLOAD_COLUMN_KEY)).thenReturn(TEST_MESSAGE_PAYLOAD);
        when(resultSet.getLong(Queue.MESSAGE_ID_COLUMN_KEY)).thenReturn(TEST_MESSAGE_ID);
        IMessage message = queue.get();
        assertEquals(message.getPayload(), TEST_MESSAGE_PAYLOAD);
        assertEquals(((Message) message).getId(), TEST_MESSAGE_ID);
        verify(connection, times(1)).setAutoCommit(false);
        verify(connection, times(0)).commit();
        verify(connection, times(0)).setAutoCommit(true);
    }

    @Test
    public void getWhenMessageIsNotExistsTest() throws Exception {
        when(connection.prepareStatement(Queue.GET_FIRST_MESSAGE_SQL)).thenReturn(preparedStatement);
        ResultSet resultSet = mock(ResultSet.class);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);
        assertNull(queue.get());
        InOrder inOrder = inOrder(connection);
        inOrder.verify(connection, times(1)).setAutoCommit(false);
        inOrder.verify(connection, times(1)).commit();
        inOrder.verify(connection, times(1)).setAutoCommit(true);
    }

    @Test(expected = ActiveMessageExistsException.class)
    public void getWhenActiveMessageExistsTest() throws NoSuchFieldException, IllegalAccessException {
        Class<?> c = queue.getClass();
        Field field = c.getDeclaredField(QUEUE_ACTIVE_MESSAGE_FILED);
        field.setAccessible(true);
        field.set(queue, new Message(TEST_MESSAGE_PAYLOAD, TEST_MESSAGE_ID));
        queue.get();
    }

    @Test
    public void ackWithoutSqlExceptionOnExecutionTest() throws SQLException, NoSuchFieldException, IllegalAccessException {
        Class<?> c = queue.getClass();
        Field field = c.getDeclaredField(QUEUE_ACTIVE_MESSAGE_FILED);
        field.setAccessible(true);
        IMessage message = (IMessage) field.get(queue);
        assertNull(message);
        field.set(queue, new Message(TEST_MESSAGE_PAYLOAD, TEST_MESSAGE_ID));

        when(connection.prepareStatement(Queue.DELETE_MESSAGE_SQL)).thenReturn(preparedStatement);

        boolean result = queue.ack();

        InOrder inOrder = inOrder(connection, preparedStatement);
        inOrder.verify(connection, times(1)).prepareStatement(Queue.DELETE_MESSAGE_SQL);
        inOrder.verify(preparedStatement, times(1)).executeUpdate();
        inOrder.verify(connection, times(1)).commit();
        inOrder.verify(connection, times(1)).setAutoCommit(true);

        assertNull(field.get(queue));
        assertTrue(result);
    }

    @Test
    public void ackWithSqlExceptionOnExecutionTest() throws SQLException {
        when(connection.prepareStatement(Queue.DELETE_MESSAGE_SQL)).thenThrow(SQLException.class);

        boolean result = queue.ack();

        InOrder inOrder = inOrder(connection, preparedStatement);
        inOrder.verify(connection, times(1)).prepareStatement(Queue.DELETE_MESSAGE_SQL);
        inOrder.verify(connection, times(1)).rollback();
        inOrder.verify(connection, times(1)).setAutoCommit(true);

        assertFalse(result);
    }

    @Test
    public void pushWithoutExceptionTest() throws SQLException {
        when(connection.prepareStatement(Queue.INSERT_MESSAGE_SQL)).thenReturn(preparedStatement);
        boolean result = queue.push(new Message(TEST_MESSAGE_PAYLOAD, TEST_MESSAGE_ID));
        assertTrue(result);
        verify(connection, times(0)).commit();
    }

    @Test
    public void pushWithExceptionTest() throws SQLException {
        when(connection.prepareStatement(Queue.INSERT_MESSAGE_SQL)).thenThrow(SQLException.class);
        boolean result = queue.push(new Message(TEST_MESSAGE_PAYLOAD, TEST_MESSAGE_ID));
        assertFalse(result);
        verify(connection, times(0)).commit();
    }
}
