package org.apache.beam.sdk.transforms;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.SchemaCoder;

@AutoValue
public abstract class DoFnSchemaInformation implements Serializable {
  @Nullable
  public abstract SchemaCoder<?> getElementParameterSchema();


  public static DoFnSchemaInformation create() {
    return new AutoValue_DoFnSchemaInformation.Builder()
        .build();
  }
  @AutoValue.Builder
  abstract public static class Builder {
    abstract Builder setElementParameterSchema(@Nullable SchemaCoder<?> schemaCoder);
    abstract DoFnSchemaInformation build();
  }
  public abstract Builder toBuilder();

  public <S> DoFnSchemaInformation withElementParameterSchema(SchemaCoder<S> schemaCoder) {
    return toBuilder().setElementParameterSchema(schemaCoder).build();
  }
}
