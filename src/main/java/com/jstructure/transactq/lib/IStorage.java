package com.jstructure.transactq.lib;

public interface IStorage {
    boolean declareQueue(String name);

    boolean deleteQueue(String name);

    IQueue getQueue(String name);
}
