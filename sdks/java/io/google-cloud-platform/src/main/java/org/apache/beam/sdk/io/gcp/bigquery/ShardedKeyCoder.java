package org.apache.beam.sdk.io.gcp.bigquery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.util.PropertyNames;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A {@link Coder} for {@link ShardedKey}, using a wrapped key {@link Coder}.
 */
@VisibleForTesting
class ShardedKeyCoder<KeyT>
    extends StandardCoder<ShardedKey<KeyT>> {
  public static <KeyT> ShardedKeyCoder<KeyT> of(Coder<KeyT> keyCoder) {
    return new ShardedKeyCoder<>(keyCoder);
  }

  @JsonCreator
  public static <KeyT> ShardedKeyCoder<KeyT> of(
       @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
           List<Coder<KeyT>> components) {
    checkArgument(components.size() == 1, "Expecting 1 component, got %s", components.size());
    return of(components.get(0));
  }

  protected ShardedKeyCoder(Coder<KeyT> keyCoder) {
    this.keyCoder = keyCoder;
    this.shardNumberCoder = VarIntCoder.of();
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(keyCoder);
  }

  @Override
  public void encode(ShardedKey<KeyT> key, OutputStream outStream, Context context)
      throws IOException {
    keyCoder.encode(key.getKey(), outStream, context.nested());
    shardNumberCoder.encode(key.getShardNumber(), outStream, context);
  }

  @Override
  public ShardedKey<KeyT> decode(InputStream inStream, Context context)
      throws IOException {
    return new ShardedKey<>(
        keyCoder.decode(inStream, context.nested()),
        shardNumberCoder.decode(inStream, context));
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    keyCoder.verifyDeterministic();
  }

  Coder<KeyT> keyCoder;
  VarIntCoder shardNumberCoder;
}
