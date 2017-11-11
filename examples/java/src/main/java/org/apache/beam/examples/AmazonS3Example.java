package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/** dummy. */
public class AmazonS3Example {

  interface ExampleOptions extends PipelineOptions {

  }

  public static void main(String[] args) {

    S3Options options = PipelineOptionsFactory.fromArgs(args).create().as(S3Options.class);
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(TextIO.read()
            .from("s3://kochava-collective-v2-test/kinglear.txt"))
        .apply(ParDo.of(new DoFn<String, String>() {
          @ProcessElement
          public void peek(ProcessContext context) {
            context.output(context.element());
            System.out.println(context.element());
          }
        }))
        .apply(TextIO.write()
            .to("s3://kochava-collective-v2-test/output.txt"));
  }
}
