package org.apache.beam.sdk.io.aws.sqs;

import com.amazonaws.services.sqs.model.Message;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.aws.sqs.SqsIO.Read;
import org.apache.beam.sdk.options.PipelineOptions;

class SqsUnboundedSource extends UnboundedSource<Message, SqsCheckpointMark> {

  private final Read spec;

  public SqsUnboundedSource(Read spec) {
    this.spec = spec;
  }

  @Override
  public List<SqsUnboundedSource> split(int desiredNumSplits, PipelineOptions options) {
    List<SqsUnboundedSource> sources = new ArrayList<>();
    for (int i = 0; i < Math.max(1, desiredNumSplits); ++i) {
      sources.add(new SqsUnboundedSource(spec));
    }
    return sources;
  }

  @Override
  public UnboundedReader<Message> createReader(
      PipelineOptions options, @Nullable SqsCheckpointMark checkpointMark) {
    //todo need to add the ability to resume from a checkpoint
    return new SqsUnboundedReader(this);
  }

  @Override
  public Coder<SqsCheckpointMark> getCheckpointMarkCoder() {
    return AvroCoder.of(SqsCheckpointMark.class);
  }

  @Override
  public Coder<Message> getOutputCoder() {

    return SerializableCoder.of(Message.class);
  }

  public Read getSpec() {
    return spec;
  }

  @Override
  public boolean requiresDeduping() {
    return true;
  }
}
