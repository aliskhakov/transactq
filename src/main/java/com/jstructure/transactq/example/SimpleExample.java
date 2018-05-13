package com.jstructure.transactq.example;

import com.jstructure.transactq.Message;
import com.jstructure.transactq.Storage;
import com.jstructure.transactq.connection.ConnectionFactory;
import com.jstructure.transactq.lib.IMessage;
import com.jstructure.transactq.lib.IQueue;
import com.jstructure.transactq.lib.IStorage;

import java.sql.Connection;

public class SimpleExample {
    private static final String SRC_QUEUE_NAME = "testQueue";
    private static final String DEST_QUEUE_NAME = "testQueue2";

    public static void main(String[] args) {
        ConnectionFactory conManagerFactory = ConnectionFactory.getInstance();
        Connection con = conManagerFactory.createConnection(args[0], args[1], args[2]);

        IStorage storage = new Storage(con);
        storage.declareQueue(SRC_QUEUE_NAME);
        storage.declareQueue(DEST_QUEUE_NAME);
        IQueue srcQueue = storage.getQueue(SRC_QUEUE_NAME);
        IQueue destQueue = storage.getQueue(DEST_QUEUE_NAME);
        for (int i = 0; i < 100; i++) {
            srcQueue.push(new Message("{\"key\": " + i + "}"));
        }
        try {
            IMessage message;
            while ((message = srcQueue.get()) != null) {
                System.out.println(message.getPayload());
                destQueue.push(new Message(message.getPayload()));
                srcQueue.ack();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
