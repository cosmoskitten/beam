package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.io.CountingOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.TableRowJsonCoder;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by relax on 3/17/17.
 */
class TableRowWriter {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIO.class);

  private static final Coder<TableRow> CODER = TableRowJsonCoder.of();
  private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
  private final String tempFilePrefix;
  private String id;
  private String fileName;
  private WritableByteChannel channel;
  protected String mimeType = MimeTypes.TEXT;
  private CountingOutputStream out;

  TableRowWriter(String basename) {
    this.tempFilePrefix = basename;
  }

  public final void open(String uId) throws Exception {
    id = uId;
    fileName = tempFilePrefix + id;
    LOG.debug("Opening {}.", fileName);
    channel = IOChannelUtils.create(fileName, mimeType);
    try {
      out = new CountingOutputStream(Channels.newOutputStream(channel));
      LOG.debug("Writing header to {}.", fileName);
    } catch (Exception e) {
      try {
        LOG.error("Writing header to {} failed, closing channel.", fileName);
        channel.close();
      } catch (IOException closeException) {
        LOG.error("Closing channel for {} failed", fileName);
      }
      throw e;
    }
    LOG.debug("Starting write of bundle {} to {}.", this.id, fileName);
  }

  public void write(TableRow value) throws Exception {
    CODER.encode(value, out, Context.OUTER);
    out.write(NEWLINE);
  }

  public final KV<String, Long> close() throws IOException {
    channel.close();
    return KV.of(fileName, out.getCount());
  }
}
