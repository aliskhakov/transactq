package com.jstructure.transactq.example;

import com.jstructure.transactq.Message;
import com.jstructure.transactq.Storage;
import com.jstructure.transactq.Worker;
import com.jstructure.transactq.WorkerPool;
import com.jstructure.transactq.connection.ConnectionFactory;
import com.jstructure.transactq.lib.IMessage;
import com.jstructure.transactq.lib.IQueue;
import com.jstructure.transactq.lib.IStorage;
import com.jstructure.transactq.lib.IWorkerPool;

import java.sql.Connection;

public class WorkerPoolExample {
    private static final String SRC_QUEUE_NAME = "testQueue";
    private static final String DEST_QUEUE_NAME = "testQueue2";

    static class MyWorker extends Worker {
        private final IQueue destQueue;

        MyWorker(Connection connection, String srcQueueName, String destQueueName) {
            super(connection, srcQueueName, 1);
            this.destQueue = storage.getQueue(destQueueName);
        }

        @Override
        public void work(IMessage message) {
            System.out.println(message.getPayload());
            destQueue.push(new Message(message.getPayload()));
        }

        /**
         * In the example we interrupt the thread if there
         * is no message in queue. So process will finished
         * after all workers become interrupted and if method
         * {@link WorkerPool#shutdown()} calling.
         */
        @Override
        protected void processNullMessage() {
            if (!Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        Connection connection = connectionFactory.createConnection(args[0], args[1], args[2]);

        IStorage storage = new Storage(connection);
        storage.declareQueue(SRC_QUEUE_NAME);
        IQueue sourceQueue = storage.getQueue(SRC_QUEUE_NAME);

        for (int i = 0; i < 100; i++) {
            sourceQueue.push(new Message("{\"key\": " + i + "}"));
        }

        Thread.sleep(1000);

        IWorkerPool workerPool = new WorkerPool(4);
        for (int i = 0; i < 4; i++) {
            workerPool.processWorker(new MyWorker(connectionFactory.createConnection(args[0], args[1], args[2]),
                    SRC_QUEUE_NAME, DEST_QUEUE_NAME));
        }
        workerPool.shutdown();
    }
}
