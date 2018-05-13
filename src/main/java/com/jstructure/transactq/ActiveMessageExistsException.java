package com.jstructure.transactq;

public class ActiveMessageExistsException extends RuntimeException {
    public ActiveMessageExistsException() {
        super("You have not acked message. Ack it before.");
    }
}
