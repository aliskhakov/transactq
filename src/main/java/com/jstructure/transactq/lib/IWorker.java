package com.jstructure.transactq.lib;

public interface IWorker extends Runnable {
    void work();
    void work(IMessage message);
}
