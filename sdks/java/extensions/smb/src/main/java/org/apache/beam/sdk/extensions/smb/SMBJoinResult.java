package org.apache.beam.sdk.extensions.smb;

import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TupleTag;

public class SMBJoinResult {
  private final Map<TupleTag, Iterable<?>> valueMap;

  SMBJoinResult(Map<TupleTag, Iterable<?>> valueMap) {
    this.valueMap = valueMap;
  }

  public <V> Iterable<V> getValuesForTag(TupleTag<V> tag) {
    return (Iterable<V>) valueMap.get(tag);
  }

  public static abstract class ToResult<ResultT> implements SerializableFunction<SMBJoinResult, ResultT> {
    public abstract Coder<ResultT> resultCoder();
  }

}
