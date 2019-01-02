package org.apache.beam.sdk.transforms;

import com.google.auto.value.AutoValue;
import com.google.auto.value.AutoValue.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

@AutoValue
public abstract class DoFnSchemaInformation implements Serializable {
  @Nullable
  public abstract SchemaCoder<?> getElementParameterSchema();

  public abstract Map<TupleTag<?>, SerializableFunction<?, Row>> getOutputReceiverToRows();

  public abstract Map<TupleTag<?>, SerializableFunction<Coder<?>, Boolean>>
  getOutputSchemaRestrictions();

  public SerializableFunction<?, Row> getOutputReceiverToRow(TupleTag<?> tag) {
    return getOutputReceiverToRows().get(tag);
  }

  public static DoFnSchemaInformation create() {
    return new AutoValue_DoFnSchemaInformation.Builder()
        .setOutputReceiverToRows(Collections.emptyMap())
        .setOutputSchemaRestrictions(Collections.emptyMap())
        .build();
  }
  @AutoValue.Builder
  abstract public static class Builder {
    abstract Builder setElementParameterSchema(@Nullable SchemaCoder<?> schemaCoder);
    abstract Builder setOutputReceiverToRows(Map<TupleTag<?>, SerializableFunction<?, Row>> toRows);
    abstract Builder setOutputSchemaRestrictions(
        Map<TupleTag<?>, SerializableFunction<Coder<?>, Boolean>> restrictions);
    abstract DoFnSchemaInformation build();
  }
  public abstract Builder toBuilder();

  public <S> DoFnSchemaInformation withElementParameterSchema(SchemaCoder<S> schemaCoder) {
    return toBuilder().setElementParameterSchema(schemaCoder).build();
  }

  public DoFnSchemaInformation withOutputReceiverToRow(
      TupleTag<?> tag, SerializableFunction<?, Row> toRow) {
    ImmutableMap<TupleTag<?>, SerializableFunction<?, Row>> newMap =
        ImmutableMap.<TupleTag<?>, SerializableFunction<?, Row>>builder()
        .putAll(getOutputReceiverToRows())
        .put(tag, toRow)
        .build();
    return toBuilder().setOutputReceiverToRows(newMap).build();
  }

  public DoFnSchemaInformation withOutputSchemaRestriction(
      TupleTag<?> tag, SerializableFunction<Coder<?>, Boolean> predicate) {
    ImmutableMap<TupleTag<?>, SerializableFunction<Coder<?>, Boolean>> newMap =
        ImmutableMap.<TupleTag<?>, SerializableFunction<Coder<?>, Boolean>>builder()
            .putAll(getOutputSchemaRestrictions())
            .put(tag, predicate)
            .build();
    return toBuilder().setOutputSchemaRestrictions(newMap).build();
  }
}
