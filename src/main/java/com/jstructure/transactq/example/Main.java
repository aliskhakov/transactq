package com.jstructure.transactq.example;

import com.jstructure.transactq.Message;
import com.jstructure.transactq.Storage;
import com.jstructure.transactq.connection.ConnectionManagerFactory;
import com.jstructure.transactq.lib.IConnectionManager;
import com.jstructure.transactq.lib.IMessage;
import com.jstructure.transactq.lib.IQueue;
import com.jstructure.transactq.lib.IStorage;

import java.sql.Connection;

public class Main {
    public static void main(String[] args) {
        ConnectionManagerFactory connectionManagerFactory = ConnectionManagerFactory.getInstance();
        IConnectionManager connectionManager = connectionManagerFactory.createConnectionManager(args[0], args[1],
                args[2]);
        Connection connection = connectionManager.getConnection();

        IStorage storage = new Storage(connection);
        storage.declareQueue("testQueue");
        IQueue queue = storage.getQueue("testQueue");
        for (int i = 0; i < 100; i++) {
            queue.push(new Message("{\"key\": " + i + "}"));
        }
        try {
            IMessage message;
            while ((message = queue.get()) != null) {
                System.out.println(message.getPayload());
                queue.ack();
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
