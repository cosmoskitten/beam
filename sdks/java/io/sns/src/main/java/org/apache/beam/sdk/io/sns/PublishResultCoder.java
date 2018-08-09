package org.apache.beam.sdk.io.sns;

import com.amazonaws.services.sns.model.PublishResult;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/** Custom Coder for handling publish result. */
public class PublishResultCoder extends Coder<PublishResult> {
  @Override
  public void encode(PublishResult value, OutputStream outStream)
      throws CoderException, IOException {
    StringUtf8Coder.of().encode(value.getMessageId(), outStream);
  }

  @Override
  public PublishResult decode(InputStream inStream) throws CoderException, IOException {
    final String messageId = StringUtf8Coder.of().decode(inStream);
    return new PublishResult().withMessageId(messageId);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return ImmutableList.of();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    StringUtf8Coder.of().verifyDeterministic();
  }
}
