package org.apache.beam.sdk.io;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import org.apache.beam.sdk.values.PCollectionView;

public interface FileSink<DestinationT, ElementT> extends Serializable {
  void open(DestinationT destination, WritableByteChannel channel) throws IOException;
  void write(ElementT element) throws IOException;
  void finish() throws IOException;
}
