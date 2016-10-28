package org.apache.beam.sdk.util;

import com.google.rpc.Status;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by peihe on 10/28/16.
 */
public interface IOChannelFactoryV2 {
  // REQUIRED FUNCTIONALITIES:
  //   Throws IOExceptions if operations have failed.

  // A. Read/Write channels APIs
  WritableByteChannel create(String uri, CreateOptions options);
  ReadableByteChannel open(String uri, OpenOptions options);


  // B. Files management API without NameTemplate.
  void delete(String uri);
  void copy(String sourceUri, String destinationUri);
  void rename(String sourceUri, String destinationUri);
  Collection<Metadata> match(String glob);
  String resolve(String path, String other);
  boolean isReadSeekEfficient(String spec);

  // OPTIONAL FUNCTIONALITIES:
  //   Do not throw if operations are not supported, instead return to indicate
  //   requests have failed.


  // D. Bulk operation.
  // Returns failed uris.
  Set<String> bulkDelete(Set<String> uris);
  Map<String, String> bulkCopy(Map<String, String> uris);
  Map<String, String> bulkRename(Map<String, String> uris);
  // Returns succeeded uris and their Metadata.
  Collection<Collection<Metadata>> getBulkMetadata(List<String> globs);

  class CreateOptions {
  }

  class OpenOptions {
  }

  class Metadata {
    public Status status;
    public String uri;
    public long sizeBytes;
  }

  enum Status {
    OK,
    UNKNOWN,
  }
}
