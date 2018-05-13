package com.jstructure.transactq;

import com.jstructure.transactq.lib.IMessage;
import com.jstructure.transactq.lib.IQueue;
import com.jstructure.transactq.lib.IStorage;
import com.jstructure.transactq.lib.IWorker;

import java.sql.Connection;


public abstract class Worker implements IWorker {
    protected long getInterval = 1000;
    protected IStorage storage;
    protected IQueue queue;

    public Worker(Connection connection, String queueName) {
        this.initStorage(connection, queueName);
    }

    public Worker(Connection connection, String queueName, long getInterval) {
        this.initStorage(connection, queueName);
        this.getInterval = getInterval;
    }

    private void initStorage(Connection connection, String queueName) {
        storage = new Storage(connection);
        queue = storage.getQueue(queueName);
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                work();
                Thread.sleep(getInterval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void work() {
        IMessage message = queue.get();
        if (message != null) {
            work(message);
            queue.ack();
        } else {
            processNullMessage();
        }
    }

    protected void processNullMessage() {
    }
}
