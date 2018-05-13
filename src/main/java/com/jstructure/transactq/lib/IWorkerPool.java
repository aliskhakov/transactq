package com.jstructure.transactq.lib;

public interface IWorkerPool {
    void processWorker(IWorker worker);
    void shutdown();
}
