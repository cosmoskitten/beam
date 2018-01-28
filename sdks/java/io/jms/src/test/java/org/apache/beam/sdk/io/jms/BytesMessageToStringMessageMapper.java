package org.apache.beam.sdk.io.jms;

import javax.jms.BytesMessage;
import javax.jms.Message;

/**
 * A test class that maps a {@link javax.jms.BytesMessage} into a {@link String}.
 */
public class BytesMessageToStringMessageMapper implements JmsIO.MessageMapper<String> {

  @Override public String mapMessage(Message message) throws Exception {
    BytesMessage bytesMessage = (BytesMessage) message;

    byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];

    return new String(bytes);
  }
}
