package com.jstructure.transactq;

import com.jstructure.transactq.lib.IMessage;

public class Message implements IMessage {
    private String payload;
    private long id;

    Message(String payload, long id) {
        this.payload = payload;
        this.id = id;
    }

    public Message(String payload) {
        this.payload = payload;
    }

    @Override
    public String getPayload() {
        return payload;
    }

    long getId() {
        return id;
    }
}
