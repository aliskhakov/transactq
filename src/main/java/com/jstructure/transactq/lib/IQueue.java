package com.jstructure.transactq.lib;

import java.util.List;

public interface IQueue {
    List<IMessage> get(int limit);

    IMessage get();

    boolean ack();

    boolean push(IMessage message);
}
