package org.apache.beam.sdk.io.gcp.spanner;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

class SerializedMutationCoder extends AtomicCoder<SerializedMutation> {

  private static final SerializedMutationCoder INSTANCE = new SerializedMutationCoder();

  public static SerializedMutationCoder of() {
    return INSTANCE;
  }

  private final ByteArrayCoder byteArrayCoder;
  private final StringUtf8Coder stringCoder;

  private SerializedMutationCoder() {
    byteArrayCoder = ByteArrayCoder.of();
    stringCoder = StringUtf8Coder.of();
  }

  @Override
  public void encode(SerializedMutation value, OutputStream out)
      throws IOException {
    stringCoder.encode(value.getTableName(), out);
    byteArrayCoder.encode(value.getEncodedKey(), out);
    byteArrayCoder.encode(value.getMutationGroupBytes(), out);
  }

  @Override
  public SerializedMutation decode(InputStream in)
      throws IOException {
    String tableName =  stringCoder.decode(in);
    byte[] encodedKey = byteArrayCoder.decode(in);
    byte[] mutationBytes = byteArrayCoder.decode(in);
    return SerializedMutation.create(tableName, encodedKey, mutationBytes);
  }

}
