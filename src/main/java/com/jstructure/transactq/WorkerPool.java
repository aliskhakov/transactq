package com.jstructure.transactq;

import com.jstructure.transactq.lib.IWorker;
import com.jstructure.transactq.lib.IWorkerPool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WorkerPool implements IWorkerPool {
    private final ExecutorService pool;

    public WorkerPool(int size) {
        pool = Executors.newFixedThreadPool(size);
    }

    @Override
    public void processWorker(IWorker worker) {
        pool.execute(worker);
    }

    @Override
    public void shutdown() {
        pool.shutdown();
    }
}
