package com.jstructure.transactq;

import com.jstructure.transactq.lib.IMessage;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class MessageTest {
    private static final String PAYLOAD = "Payload";
    private static final long ID = 1;

    @Test
    public void getPayloadOfNotPersistedMessageTest() {
        IMessage message = new Message(PAYLOAD);
        String payload = message.getPayload();
        assertEquals(payload, PAYLOAD);
    }

    @Test
    public void getPayloadOfPersistedMessageTest() {
        IMessage message = new Message(PAYLOAD, ID);
        String payload = message.getPayload();
        assertEquals(payload, PAYLOAD);
    }

    @Test
    public void getIdOfNotPersistedMessageTest() {
        IMessage message = new Message(PAYLOAD);
        long id = ((Message) message).getId();
        assertEquals(id, 0);
    }

    @Test
    public void getIdOfPersistedMassageTest() {
        IMessage message = new Message(PAYLOAD, ID);
        long id = ((Message) message).getId();
        assertEquals(id, ID);
    }
}
