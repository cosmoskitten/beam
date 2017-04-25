/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * {@link PTransform}s for reading and writing Avro files.
 *
 * <p>To read a {@link PCollection} from one or more Avro files, use
 * {@link AvroIO.Read}, specifying {@link AvroIO.Read#from} to specify
 * the path of the file(s) to read from (e.g., a local filename or
 * filename pattern if running locally, or a Google Cloud Storage
 * filename or filename pattern of the form {@code "gs://<bucket>/<filepath>"}).
 *
 * <p>It is required to specify {@link AvroIO.Read#withSchema}. To
 * read specific records, such as Avro-generated classes, provide an
 * Avro-generated class type. To read {@link GenericRecord GenericRecords}, provide either
 * a {@link Schema} object or an Avro schema in a JSON-encoded string form.
 * An exception will be thrown if a record doesn't match the specified
 * schema.
 *
 * <p>For example:
 * <pre> {@code
 * Pipeline p = ...;
 *
 * // A simple Read of a local file (only runs locally):
 * PCollection<AvroAutoGenClass> records =
 *     p.apply(AvroIO.Read.from("/path/to/file.avro")
 *                 .withSchema(AvroAutoGenClass.class));
 *
 * // A Read from a GCS file (runs locally and using remote execution):
 * Schema schema = new Schema.Parser().parse(new File("schema.avsc"));
 * PCollection<GenericRecord> records =
 *     p.apply(AvroIO.Read
 *                .from("gs://my_bucket/path/to/records-*.avro")
 *                .withSchema(schema));
 * } </pre>
 *
 * <p>To write a {@link PCollection} to one or more Avro files, use {@link Write}, specifying
 * {@link Write#to(String)} to specify the prefix of the files to write.
 * {@link Write.Bound#withFilenamePolicy(FilenamePolicy)} can also be used to specify a custom file
 * naming policy.
 *
 * <p>By default, all input is put into the global window before writing. If per-window writes are
 * desired - for example, when using a streaming runner -
 * {@link AvroIO.Write.Bound#withWindowedWrites()} will cause windowing and triggering to be
 * preserved. When producing windowed writes, the number of output shards must be set explicitly
 * using {@link AvroIO.Write.Bound#withNumShards(int)}; some runners may set this for you to a
 * runner-chosen value, so you may need not set it yourself. A
 * {@link FileBasedSink.FilenamePolicy} must be set, and unique windows and triggers must produce
 * unique filenames.
 *
 * <p>It is required to specify {@link AvroIO.Write.Bound#withSchema}. To
 * write specific records, such as Avro-generated classes, provide an
 * Avro-generated class type. To write {@link GenericRecord GenericRecords}, provide either
 * a {@link Schema} object or a schema in a JSON-encoded string form.
 * An exception will be thrown if a record doesn't match the specified
 * schema.
 *
 * <p>For example:
 * <pre> {@code
 * // A simple Write to a local file (only runs locally):
 * PCollection<AvroAutoGenClass> records = ...;
 * records.apply(AvroIO.Write.to("/path/to/file.avro")
 *                           .withSchema(AvroAutoGenClass.class));
 *
 * // A Write to a sharded GCS file (runs locally and using remote execution):
 * Schema schema = new Schema.Parser().parse(new File("schema.avsc"));
 * PCollection<GenericRecord> records = ...;
 * records.apply("WriteToAvro", AvroIO.Write
 *     .to("gs://my_bucket/path/to/numbers")
 *     .withSchema(schema)
 *     .withSuffix(".avro"));
 * } </pre>
 *
 * <p>By default, {@link AvroIO.Write} produces output files that are compressed using the
 * {@link org.apache.avro.file.Codec CodecFactory.deflateCodec(6)}. This default can
 * be changed or overridden using {@link AvroIO.Write.Bound#withCodec}.
 *
 * <h3>Permissions</h3>
 * Permission requirements depend on the {@link PipelineRunner} that is used to execute the
 * pipeline. Please refer to the documentation of corresponding {@link PipelineRunner}s for
 * more details.
 */
public class AvroIO {
  /**
   * A root {@link PTransform} that reads from an Avro file (or multiple Avro
   * files matching a pattern) and returns a {@link PCollection} containing
   * the decoding of each record.
   */
  public static class Read {

    /**
     * Returns a {@link PTransform} that reads from the file(s)
     * with the given name or pattern. This can be a local filename
     * or filename pattern (if running locally), or a Google Cloud
     * Storage filename or filename pattern of the form
     * {@code "gs://<bucket>/<filepath>"} (if running locally or
     * using remote execution). Standard
     * <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html">Java
     * Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
     */
    public static Bound<GenericRecord> from(String filepattern) {
      return new Bound<>(GenericRecord.class).from(filepattern);
    }

    /**
     * Returns a {@link PTransform} that reads Avro file(s)
     * containing records whose type is the specified Avro-generated class.
     *
     * @param <T> the type of the decoded elements, and the elements
     * of the resulting {@link PCollection}
     */
    public static <T> Bound<T> withSchema(Class<T> type) {
      return new Bound<>(type).withSchema(type);
    }

    /**
     * Returns a {@link PTransform} that reads Avro file(s)
     * containing records of the specified schema.
     */
    public static Bound<GenericRecord> withSchema(Schema schema) {
      return new Bound<>(GenericRecord.class).withSchema(schema);
    }

    /**
     * Returns a {@link PTransform} that reads Avro file(s)
     * containing records of the specified schema in a JSON-encoded
     * string form.
     */
    public static Bound<GenericRecord> withSchema(String schema) {
      return withSchema((new Schema.Parser()).parse(schema));
    }

    /**
     * A {@link PTransform} that reads from an Avro file (or multiple Avro
     * files matching a pattern) and returns a bounded {@link PCollection} containing
     * the decoding of each record.
     *
     * @param <T> the type of each of the elements of the resulting
     * PCollection
     */
    public static class Bound<T> extends PTransform<PBegin, PCollection<T>> {
      /** The filepattern to read from. */
      @Nullable
      final String filepattern;
      /** The class type of the records. */
      final Class<T> type;
      /** The schema of the input file. */
      @Nullable
      final Schema schema;

      Bound(Class<T> type) {
        this(null, null, type, null);
      }

      Bound(String name, String filepattern, Class<T> type, Schema schema) {
        super(name);
        this.filepattern = filepattern;
        this.type = type;
        this.schema = schema;
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that reads from the file(s) with the given name or pattern.
       * (See {@link AvroIO.Read#from} for a description of
       * filepatterns.)
       *
       * <p>Does not modify this object.
       */
      public Bound<T> from(String filepattern) {
        return new Bound<>(name, filepattern, type, schema);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that reads Avro file(s) containing records whose type is the
       * specified Avro-generated class.
       *
       * <p>Does not modify this object.
       *
       * @param <X> the type of the decoded elements and the elements of
       * the resulting PCollection
       */
      public <X> Bound<X> withSchema(Class<X> type) {
        return new Bound<>(name, filepattern, type, ReflectData.get().getSchema(type));
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that reads Avro file(s) containing records of the specified schema.
       *
       * <p>Does not modify this object.
       */
      public Bound<GenericRecord> withSchema(Schema schema) {
        return new Bound<>(name, filepattern, GenericRecord.class, schema);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that reads Avro file(s) containing records of the specified schema
       * in a JSON-encoded string form.
       *
       * <p>Does not modify this object.
       */
      public Bound<GenericRecord> withSchema(String schema) {
        return withSchema((new Schema.Parser()).parse(schema));
      }

      @Override
      public PCollection<T> expand(PBegin input) {
        if (filepattern == null) {
          throw new IllegalStateException(
              "need to set the filepattern of an AvroIO.Read transform");
        }
        if (schema == null) {
          throw new IllegalStateException("need to set the schema of an AvroIO.Read transform");
        }

        @SuppressWarnings("unchecked")
        Bounded<T> read =
            type == GenericRecord.class
                ? (Bounded<T>) org.apache.beam.sdk.io.Read.from(
                    AvroSource.from(filepattern).withSchema(schema))
                : org.apache.beam.sdk.io.Read.from(
                    AvroSource.from(filepattern).withSchema(type));

        PCollection<T> pcol = input.getPipeline().apply("Read", read);
        // Honor the default output coder that would have been used by this PTransform.
        pcol.setCoder(getDefaultOutputCoder());
        return pcol;
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder
          .addIfNotNull(DisplayData.item("filePattern", filepattern)
            .withLabel("Input File Pattern"));
      }

      @Override
      protected Coder<T> getDefaultOutputCoder() {
        return AvroCoder.of(type, schema);
      }

      public String getFilepattern() {
        return filepattern;
      }

      public Schema getSchema() {
        return schema;
      }
    }

    /** Disallow construction of utility class. */
    private Read() {}
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A root {@link PTransform} that writes a {@link PCollection} to an Avro file (or
   * multiple Avro files matching a sharding pattern).
   */
  public static class Write {

    /**
     * Returns a transform for writing to Avro files that writes to file(s) with the given output
     * prefix. The given {@code prefix} can reference any {@link FileSystem} on the classpath.
     *
     * <p>The name of the output files will be determined by the {@link FilenamePolicy} used.
     *
     * <p>By default, a {@link DefaultFilenamePolicy} will be used built using the specified prefix
     * to define the base output directory and file prefix, a shard identifier (see
     * {@link Bound#withNumShards(int)}), and a common suffix (if supplied using
     * {@link Bound#withSuffix(String)}).
     */
    public static Bound<GenericRecord> to(String outputPrefix) {
      return new Bound<>(GenericRecord.class).to(outputPrefix);
    }

    /**
     * Returns a transform for writing to Avro files that writes to file(s) in paths identified by
     * the given resource. The given {@code outputResource} can reference any {@link FileSystem} on
     * the classpath.
     *
     * <p>The provided resource will be used to determine the base directory for output.The name of
     * the output files will be determined by the {@link FilenamePolicy} used.
     *
     * <p>By default, a {@link DefaultFilenamePolicy} will be used. If present, the
     * {@link ResourceId#getFilename() filename} of the output resource will be used to set the file
     * prefix. Files are additionally named by a shard identifier (see
     * {@link Bound#withNumShards(int)}) and can be configured with a common suffix using
     * {@link Bound#withSuffix(String)}.
     *
     * <p>If a custom {@link FilenamePolicy} is set using {@link Bound#withFilenamePolicy}, then
     * any filename on the output resource will be ignored, as the {@link FilenamePolicy} determines
     * the filename.
     */
    public static Bound<GenericRecord> to(ResourceId outputResource) {
      return new Bound<>(GenericRecord.class).to(outputResource);
    }

    /**
     * A {@link PTransform} that writes a bounded {@link PCollection} to an Avro file (or
     * multiple Avro files matching a sharding pattern).
     *
     * @param <T> the type of each of the elements of the input PCollection
     */
    public static class Bound<T> extends PTransform<PCollection<T>, PDone> {
      private static final SerializableAvroCodecFactory DEFAULT_CODEC =
          new SerializableAvroCodecFactory(CodecFactory.deflateCodec(6));
      // This should be a multiple of 4 to not get a partial encoded byte.
      private static final int METADATA_BYTES_MAX_LENGTH = 40;

      /** The output directory to write to. */
      @Nullable final ValueProvider<ResourceId> outputPrefix;
      /** Shard template string. */
      @Nullable final String shardTemplate;
      /** Suffix to use for each filename. */
      @Nullable final String filenameSuffix;
      /** Requested number of shards. 0 for automatic. */
      final int numShards;
      /** The class type of the records. */
      final Class<T> type;
      /** The schema of the output file. */
      @Nullable final Schema schema;
      final boolean windowedWrites;
      @Nullable final FileBasedSink.FilenamePolicy filenamePolicy;

      /**
       * The codec used to encode the blocks in the Avro file. String value drawn from those in
       * https://avro.apache.org/docs/1.7.7/api/java/org/apache/avro/file/CodecFactory.html
       */
      final SerializableAvroCodecFactory codec;
      /** Avro file metadata. */
      final ImmutableMap<String, Object> metadata;

      Bound(Class<T> type) {
        this(
            null,
            null,
            null,
            0,
            type,
            null,
            DEFAULT_CODEC,
            ImmutableMap.<String, Object>of(),
            false,
            null);
      }

      Bound(
          @Nullable ValueProvider<ResourceId> outputPrefix,
          @Nullable String shardTemplate,
          @Nullable String filenameSuffix,
          int numShards,
          @Nullable Class<T> type,
          @Nullable Schema schema,
          @Nullable SerializableAvroCodecFactory codec,
          Map<String, Object> metadata,
          boolean windowedWrites,
          @Nullable FilenamePolicy filenamePolicy) {
        this.outputPrefix = outputPrefix;
        this.filenameSuffix = filenameSuffix;
        this.numShards = numShards;
        this.shardTemplate = shardTemplate;
        this.type = type;
        this.schema = schema;
        this.codec = codec;
        this.windowedWrites = windowedWrites;
        this.filenamePolicy = filenamePolicy;

        Map<String, String> badKeys = Maps.newLinkedHashMap();
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
          Object v = entry.getValue();
          if (!(v instanceof String || v instanceof Long || v instanceof byte[])) {
            badKeys.put(entry.getKey(), v.getClass().getSimpleName());
          }
        }
        checkArgument(
            badKeys.isEmpty(),
            "Metadata value type must be one of String, Long, or byte[]. Found {}", badKeys);
        this.metadata = ImmutableMap.copyOf(metadata);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but that writes files to the given
       * output directory.
       *
       * <p>See {@link AvroIO.Write#to(String)} for more information about filenames.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> to(String outputPrefix) {
        try {
          // Try to interpret this string as a filename so that, e.g., /some/dir/file- supports
          // /some/dir as the output directory and file- as the prefix.
          //
          // But if this fails, say for /some/dir/ , fallback to treating it as a directory.
          return to(FileSystems.matchNewResource(outputPrefix, false /* isDirectory */));
        } catch (Exception e) {
          return to(FileSystems.matchNewResource(outputPrefix, true /* isDirectory */));
        }
      }

      /**
       * Returns a new {@link PTransform} that's like this one but that writes files to the given
       * output directory.
       *
       * <p>See {@link AvroIO.Write#to(String)} for more information about filenames.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> to(ResourceId outputDirectory) {
        return new Bound<>(
            StaticValueProvider.of(outputDirectory),
            shardTemplate,
            filenameSuffix,
            numShards,
            type,
            schema,
            codec,
            metadata,
            windowedWrites,
            filenamePolicy);
      }

      public Bound<T> withFilenamePolicy(FileBasedSink.FilenamePolicy filenamePolicy) {
        return new Bound<>(
            outputPrefix,
            shardTemplate,
            filenameSuffix,
            numShards,
            type,
            schema,
            codec,
            metadata,
            windowedWrites,
            filenamePolicy);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that writes to the file(s) with the given filename suffix.
       *
       * <p>See {@link ShardNameTemplate} for a description of shard templates.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> withSuffix(String filenameSuffix) {
        return new Bound<>(
            outputPrefix,
            shardTemplate,
            filenameSuffix,
            numShards,
            type,
            schema,
            codec,
            metadata,
            windowedWrites,
            filenamePolicy);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that uses the provided shard count.
       *
       * <p>Constraining the number of shards is likely to reduce
       * the performance of a pipeline. Setting this value is not recommended
       * unless you require a specific number of output files.
       *
       * <p>Does not modify this object.
       *
       * @param numShards the number of shards to use, or 0 to let the system
       *                  decide.
       * @see ShardNameTemplate
       */
      public Bound<T> withNumShards(int numShards) {
        checkArgument(numShards >= 0);
        return new Bound<>(
            outputPrefix,
            shardTemplate,
            filenameSuffix,
            numShards,
            type,
            schema,
            codec,
            metadata,
            windowedWrites,
            filenamePolicy);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that uses the given shard name template.
       *
       * <p>Does not modify this object.
       *
       * @see ShardNameTemplate
       */
      public Bound<T> withShardNameTemplate(String shardTemplate) {
        return new Bound<>(
            outputPrefix,
            shardTemplate,
            filenameSuffix,
            numShards,
            type,
            schema,
            codec,
            metadata,
            windowedWrites,
            filenamePolicy);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but that forces a single file as
       * output.
       *
       * <p>This is a shortcut for {@code .withNumShards(1).withShardNameTemplate("")}.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> withoutSharding() {
        return new Bound<>(
            outputPrefix,
            "",
            filenameSuffix,
            1,
            type,
            schema,
            codec,
            metadata,
            windowedWrites,
            filenamePolicy);
      }

      public Bound<T> withWindowedWrites() {
        return new Bound<>(
            outputPrefix,
            shardTemplate,
            filenameSuffix,
            numShards,
            type,
            schema,
            codec,
            metadata,
            true,
            filenamePolicy);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that writes to Avro file(s) containing records whose type is the
       * specified Avro-generated class.
       *
       * <p>Does not modify this object.
       *
       * @param <X> the type of the elements of the input PCollection
       */
      public <X> Bound<X> withSchema(Class<X> type) {
        return new Bound<>(
            outputPrefix,
            shardTemplate,
            filenameSuffix,
            numShards,
            type,
            ReflectData.get().getSchema(type),
            codec,
            metadata,
            windowedWrites,
            filenamePolicy);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that writes to Avro file(s) containing records of the specified
       * schema.
       *
       * <p>Does not modify this object.
       */
      public Bound<GenericRecord> withSchema(Schema schema) {
        return new Bound<>(
            outputPrefix,
            shardTemplate,
            filenameSuffix,
            numShards,
            GenericRecord.class,
            schema,
            codec,
            metadata,
            windowedWrites,
            filenamePolicy);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that writes to Avro file(s) containing records of the specified
       * schema in a JSON-encoded string form.
       *
       * <p>Does not modify this object.
       */
      public Bound<GenericRecord> withSchema(String schema) {
        return withSchema((new Schema.Parser()).parse(schema));
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that has GCS output path validation on pipeline creation disabled.
       *
       * <p>Does not modify this object.
       *
       * <p>This can be useful in the case where the GCS output location does
       * not exist at the pipeline creation time, but is expected to be
       * available at execution time.
       */
      public Bound<T> withoutValidation() {
        return new Bound<>(
            outputPrefix,
            shardTemplate,
            filenameSuffix,
            numShards,
            type,
            schema,
            codec,
            metadata,
            windowedWrites,
            filenamePolicy);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that writes to Avro file(s) compressed using specified codec.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> withCodec(CodecFactory codec) {
        return new Bound<>(
            outputPrefix,
            shardTemplate,
            filenameSuffix,
            numShards,
            type,
            schema,
            new SerializableAvroCodecFactory(codec),
            metadata,
            windowedWrites,
            filenamePolicy);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that writes to Avro file(s) with the specified metadata.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> withMetadata(Map<String, Object> metadata) {
        return new Bound<>(
            outputPrefix,
            shardTemplate,
            filenameSuffix,
            numShards,
            type,
            schema,
            codec,
            metadata,
            windowedWrites,
            filenamePolicy);
      }

      @Override
      public PDone expand(PCollection<T> input) {
        checkState(
            outputPrefix != null,
            "Need to set the output directory of a AvroIO.Write transform.");
        checkState(
            filenamePolicy == null || (shardTemplate == null && filenameSuffix == null),
            "Cannot set a filename policy and also a filename template or suffix.");
        checkState(schema != null, "need to set the schema of an AvroIO.Write transform");

        FilenamePolicy usedFilenamePolicy = filenamePolicy;
        if (usedFilenamePolicy == null) {
          usedFilenamePolicy = DefaultFilenamePolicy.constructUsingStandardParameters(
              outputPrefix, shardTemplate, filenameSuffix);
        }
        WriteFiles<T> write = WriteFiles.to(new AvroSink<>(
            outputPrefix, usedFilenamePolicy, AvroCoder.of(type, schema), codec, metadata));
        if (getNumShards() > 0) {
          write = write.withNumShards(getNumShards());
        }
        if (windowedWrites) {
          write = write.withWindowedWrites();
        }
        return input.apply("Write", write);
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        checkState(
            outputPrefix != null,
            "Unable to populate DisplayData for invalid AvroIO.Write (unset output prefix).");
        String outputPrefixString = null;
        if (outputPrefix.isAccessible()) {
          ResourceId dir = outputPrefix.get();
          outputPrefixString = dir.toString();
        } else {
          outputPrefixString = outputPrefix.toString();
        }
        builder
            .add(DisplayData.item("schema", type)
              .withLabel("Record Schema"))
            .addIfNotNull(DisplayData.item("filePrefix", outputPrefixString)
              .withLabel("Output File Prefix"))
            .addIfNotNull(DisplayData.item("shardNameTemplate", shardTemplate)
                .withLabel("Output Shard Name Template"))
            .addIfNotNull(DisplayData.item("fileSuffix", filenameSuffix)
                .withLabel("Output File Suffix"))
            .addIfNotDefault(DisplayData.item("numShards", numShards)
                .withLabel("Maximum Output Shards"),
                0)
            .addIfNotDefault(DisplayData.item("codec", codec.toString())
                .withLabel("Avro Compression Codec"),
                DEFAULT_CODEC.toString());
        builder.include("Metadata", new Metadata());
      }

      private class Metadata implements HasDisplayData {
        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
          for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            DisplayData.Type type = DisplayData.inferType(entry.getValue());
            if (type != null) {
              builder.add(DisplayData.item(entry.getKey(), type, entry.getValue()));
            } else {
              String base64 = BaseEncoding.base64().encode((byte[]) entry.getValue());
              String repr = base64.length() <= METADATA_BYTES_MAX_LENGTH
                  ? base64 : base64.substring(0, METADATA_BYTES_MAX_LENGTH) + "...";
              builder.add(DisplayData.item(entry.getKey(), repr));
            }
          }
        }
      }

      @Nullable public String getShardNameTemplate() {
        return shardTemplate;
      }

      @Override
      protected Coder<Void> getDefaultOutputCoder() {
        return VoidCoder.of();
      }

      public @Nullable ValueProvider<ResourceId> getOutputPrefix() {
        return outputPrefix;
      }

      public @Nullable String getShardTemplate() {
        return shardTemplate;
      }

      public int getNumShards() {
        return numShards;
      }

      public @Nullable String getFilenameSuffix() {
        return filenameSuffix;
      }

      public @Nullable Class<T> getType() {
        return type;
      }

      public @Nullable Schema getSchema() {
        return schema;
      }

      public @Nullable CodecFactory getCodec() {
        return codec.getCodec();
      }

      public Map<String, Object> getMetadata() {
        return metadata;
      }
    }

    /** Disallow construction of utility class. */
    private Write() {}
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Disallow construction of utility class. */
  private AvroIO() {}

  /**
   * A {@link FileBasedSink} for Avro files.
   */
  @VisibleForTesting
  static class AvroSink<T> extends FileBasedSink<T> {
    private final AvroCoder<T> coder;
    private final SerializableAvroCodecFactory codec;
    private final ImmutableMap<String, Object> metadata;

    @VisibleForTesting
    AvroSink(
        ValueProvider<ResourceId> outputPrefix,
        FilenamePolicy filenamePolicy,
        AvroCoder<T> coder,
        SerializableAvroCodecFactory codec,
        ImmutableMap<String, Object> metadata) {
      // Avro handle compression internally using the codec.
      super(outputPrefix, filenamePolicy, CompressionType.UNCOMPRESSED);
      this.coder = coder;
      this.codec = codec;
      this.metadata = metadata;
    }

    @Override
    public FileBasedSink.FileBasedWriteOperation<T> createWriteOperation() {
      return new AvroWriteOperation<>(this, coder, codec, metadata);
    }

    /**
     * A {@link org.apache.beam.sdk.io.FileBasedSink.FileBasedWriteOperation
     * FileBasedWriteOperation} for Avro files.
     */
    private static class AvroWriteOperation<T> extends FileBasedWriteOperation<T> {
      private final AvroCoder<T> coder;
      private final SerializableAvroCodecFactory codec;
      private final ImmutableMap<String, Object> metadata;

      private AvroWriteOperation(AvroSink<T> sink,
                                 AvroCoder<T> coder,
                                 SerializableAvroCodecFactory codec,
                                 ImmutableMap<String, Object> metadata) {
        super(sink);
        this.coder = coder;
        this.codec = codec;
        this.metadata = metadata;
      }

      @Override
      public FileBasedWriter<T> createWriter() throws Exception {
        return new AvroWriter<>(this, coder, codec, metadata);
      }
    }

    /**
     * A {@link org.apache.beam.sdk.io.FileBasedSink.FileBasedWriter FileBasedWriter}
     * for Avro files.
     */
    private static class AvroWriter<T> extends FileBasedWriter<T> {
      private final AvroCoder<T> coder;
      private DataFileWriter<T> dataFileWriter;
      private SerializableAvroCodecFactory codec;
      private final ImmutableMap<String, Object> metadata;

      public AvroWriter(FileBasedWriteOperation<T> writeOperation,
                        AvroCoder<T> coder,
                        SerializableAvroCodecFactory codec,
                        ImmutableMap<String, Object> metadata) {
        super(writeOperation, MimeTypes.BINARY);
        this.coder = coder;
        this.codec = codec;
        this.metadata = metadata;
      }

      @SuppressWarnings("deprecation") // uses internal test functionality.
      @Override
      protected void prepareWrite(WritableByteChannel channel) throws Exception {
        DatumWriter<T> datumWriter = coder.getType().equals(GenericRecord.class)
            ? new GenericDatumWriter<T>(coder.getSchema())
            : new ReflectDatumWriter<T>(coder.getSchema());

        dataFileWriter = new DataFileWriter<>(datumWriter).setCodec(codec.getCodec());
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
          Object v = entry.getValue();
          if (v instanceof String) {
            dataFileWriter.setMeta(entry.getKey(), (String) v);
          } else if (v instanceof Long) {
            dataFileWriter.setMeta(entry.getKey(), (Long) v);
          } else if (v instanceof byte[]) {
            dataFileWriter.setMeta(entry.getKey(), (byte[]) v);
          } else {
            throw new IllegalStateException(
                "Metadata value type must be one of String, Long, or byte[]. Found "
                    + v.getClass().getSimpleName());
          }
        }
        dataFileWriter.create(coder.getSchema(), Channels.newOutputStream(channel));
      }

      @Override
      public void write(T value) throws Exception {
        dataFileWriter.append(value);
      }

      @Override
      protected void finishWrite() throws Exception {
        dataFileWriter.flush();
      }
    }
  }
}
