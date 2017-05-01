package org.apache.beam.sdk.io.gcp.storage;

import com.google.auto.value.AutoValue;
import com.google.cloud.hadoop.util.AbstractGoogleAsyncWriteChannel;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.fs.CreateOptions;

/**
 * An abstract class that contains common configuration options for creating resources.
 */
@AutoValue
public abstract class GcsCreateOptions extends CreateOptions {

  /**
   * The buffer size (in bytes) to use when uploading files to GCS. Please see the documentation for
   * {@link AbstractGoogleAsyncWriteChannel#setUploadBufferSize} for more information on the
   * restrictions and performance implications of this value.
   */
  @Nullable
  public abstract Integer gcsUploadBufferSizeBytes();

  // TODO: Add other GCS options when needed.

  /**
   * Returns a {@link GcsCreateOptions.Builder}.
   */
  public static GcsCreateOptions.Builder builder() {
    return new AutoValue_GcsCreateOptions.Builder();
  }

  /**
   * A builder for {@link GcsCreateOptions}.
   */
  @AutoValue.Builder
  public abstract static class Builder extends CreateOptions.Builder<GcsCreateOptions.Builder> {
    public abstract GcsCreateOptions build();
    public abstract GcsCreateOptions.Builder setGcsUploadBufferSizeBytes(@Nullable Integer bytes);
  }
}
