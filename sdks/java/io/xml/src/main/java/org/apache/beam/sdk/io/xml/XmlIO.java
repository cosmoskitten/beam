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
package org.apache.beam.sdk.io.xml;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.Charset;
import javax.annotation.Nullable;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.ValidationEventHandler;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CompressedSource;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.OffsetBasedSource;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/** Transforms for reading and writing XML files using JAXB mappers. */
public class XmlIO {
  // CHECKSTYLE.OFF: JavadocStyle
  /**
   * Reads XML files. This source reads one or more XML files and creates a {@link PCollection} of a
   * given type. Please note the example given below.
   *
   * <p>The XML file must be of the following form, where {@code root} and {@code record} are XML
   * element names that are defined by the user:
   *
   * <pre>{@code
   * <root>
   * <record> ... </record>
   * <record> ... </record>
   * <record> ... </record>
   * ...
   * <record> ... </record>
   * </root>
   * }</pre>
   *
   * <p>Basically, the XML document should contain a single root element with an inner list
   * consisting entirely of record elements. The records may contain arbitrary XML content; however,
   * that content <b>must not</b> contain the start {@code <record>} or end {@code </record>} tags.
   * This restriction enables reading from large XML files in parallel from different offsets in the
   * file.
   *
   * <p>Root and/or record elements may additionally contain an arbitrary number of XML attributes.
   * Additionally users must provide a class of a JAXB annotated Java type that can be used convert
   * records into Java objects and vice versa using JAXB marshalling/unmarshalling mechanisms.
   * Reading the source will generate a {@code PCollection} of the given JAXB annotated Java type.
   * Optionally users may provide a minimum size of a bundle that should be created for the source.
   *
   * <p>The following example shows how to use this method in a Beam pipeline:
   *
   * <pre>{@code
   * PCollection<String> output = p.apply(XmlIO.<Record>read()
   *     .from(file.toPath().toString())
   *     .withRootElement("root")
   *     .withRecordElement("record")
   *     .withRecordClass(Record.class));
   * }</pre>
   *
   * <p>By default, UTF-8 charset is used. If your file is using a different charset, you have to
   * specify the following:
   *
   * <pre>{@code
   * PCollection<String> output = p.apply(XmlIO.<Record>read()
   *      .from(file.toPath().toString())
   *      .withRooElement("root")
   *      .withRecordElement("record")
   *      .withRecordClass(Record.class)
   *      .withCharset(StandardCharsets.ISO_8859_1));
   * }</pre>
   *
   * <p>{@link java.nio.charset.StandardCharsets} provides static references to common charsets.
   *
   * <p>Currently, only XML files that use single-byte characters are supported. Using a file that
   * contains multi-byte characters may result in data loss or duplication.
   *
   * <h3>Permissions</h3>
   *
   * <p>Permission requirements depend on the {@link PipelineRunner
   * PipelineRunner} that is used to execute the Beam pipeline. Please refer to the documentation of
   * corresponding {@link PipelineRunner PipelineRunners} for more details.
   *
   * @param <T> Type of the objects that represent the records of the XML file. The {@code
   *     PCollection} generated by this source will be of this type.
   */
  // CHECKSTYLE.ON: JavadocStyle
  public static <T> Read<T> read() {
    return new AutoValue_XmlIO_Read.Builder<T>()
        .setMinBundleSize(Read.DEFAULT_MIN_BUNDLE_SIZE)
        .setCompressionType(Read.CompressionType.AUTO)
        .setCharset("UTF-8")
        .build();
  }

  // CHECKSTYLE.OFF: JavadocStyle
  /**
   * A {@link FileBasedSink} that outputs records as XML-formatted elements. Writes a {@link
   * PCollection} of records from JAXB-annotated classes to a single file location.
   *
   * <p>Given a PCollection containing records of type T that can be marshalled to XML elements,
   * this Sink will produce a single file consisting of a single root element that contains all of
   * the elements in the PCollection.
   *
   * <p>XML Sinks are created with a base filename to write to, a root element name that will be
   * used for the root element of the output files, and a class to bind to an XML element. This
   * class will be used in the marshalling of records in an input PCollection to their XML
   * representation and must be able to be bound using JAXB annotations (checked at pipeline
   * construction time).
   *
   * <p>XML Sinks can be written to using the {@link Write} transform:
   *
   * <pre>{@code
   * p.apply(XmlIO.<Type>write()
   *      .withRecordClass(Type.class)
   *      .withRootElement(root_element)
   *      .toFilenamePrefix(output_filename));
   * }</pre>
   *
   * <p>For example, consider the following class with JAXB annotations:
   *
   * <pre>
   *  {@literal @}XmlRootElement(name = "word_count_result")
   *  {@literal @}XmlType(propOrder = {"word", "frequency"})
   *  public class WordFrequency {
   *    private String word;
   *    private long frequency;
   *
   *    public WordFrequency() { }
   *
   *    public WordFrequency(String word, long frequency) {
   *      this.word = word;
   *      this.frequency = frequency;
   *    }
   *
   *    public void setWord(String word) {
   *      this.word = word;
   *    }
   *
   *    public void setFrequency(long frequency) {
   *      this.frequency = frequency;
   *    }
   *
   *    public long getFrequency() {
   *      return frequency;
   *    }
   *
   *    public String getWord() {
   *      return word;
   *    }
   *  }
   * </pre>
   *
   * <p>The following will produce XML output with a root element named "words" from a PCollection
   * of WordFrequency objects:
   *
   * <pre>{@code
   * p.apply(XmlIO.<WordFrequency>write()
   *     .withRecordClass(WordFrequency.class)
   *     .withRootElement("words")
   *     .toFilenamePrefix(output_file));
   * }</pre>
   *
   * <p>The output of which will look like:
   *
   * <pre>{@code
   * <words>
   *
   *  <word_count_result>
   *    <word>decreased</word>
   *    <frequency>1</frequency>
   *  </word_count_result>
   *
   *  <word_count_result>
   *    <word>War</word>
   *    <frequency>4</frequency>
   *  </word_count_result>
   *
   *  <word_count_result>
   *    <word>empress'</word>
   *    <frequency>14</frequency>
   *  </word_count_result>
   *
   *  <word_count_result>
   *    <word>stoops</word>
   *    <frequency>6</frequency>
   *  </word_count_result>
   *
   *  ...
   * </words>
   * }</pre>
   *
   * <p>By default the UTF-8 charset is used. This can be overridden, for example:
   *
   * <pre>{@code
   * p.apply(XmlIO.<Type>write()
   *      .withRecordClass(Type.class)
   *      .withRootElement(root_element)
   *      .withCharset(StandardCharsets.ISO_8859_1)
   *      .toFilenamePrefix(output_filename));
   * }</pre>
   */
  // CHECKSTYLE.ON: JavadocStyle
  public static <T> Write<T> write() {
    return new AutoValue_XmlIO_Write.Builder<T>().setCharset("UTF-8").build();
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    private static final int DEFAULT_MIN_BUNDLE_SIZE = 8 * 1024;

    @Nullable
    abstract String getFileOrPatternSpec();

    @Nullable
    abstract String getRootElement();

    @Nullable
    abstract String getRecordElement();

    @Nullable
    abstract Class<T> getRecordClass();

    abstract CompressionType getCompressionType();

    abstract long getMinBundleSize();

    @Nullable
    abstract String getCharset();

    abstract Builder<T> toBuilder();

    @Nullable
    abstract ValidationEventHandler getValidationEventHandler();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setFileOrPatternSpec(String fileOrPatternSpec);

      abstract Builder<T> setRootElement(String rootElement);

      abstract Builder<T> setRecordElement(String recordElement);

      abstract Builder<T> setRecordClass(Class<T> recordClass);

      abstract Builder<T> setMinBundleSize(long minBundleSize);

      abstract Builder<T> setCompressionType(CompressionType compressionType);

      abstract Builder<T> setCharset(String charset);

      abstract Builder<T> setValidationEventHandler(ValidationEventHandler validationEventHandler);

      abstract Read<T> build();
    }

    /** Strategy for determining the compression type of XML files being read. */
    public enum CompressionType {
      /** Automatically determine the compression type based on filename extension. */
      AUTO(""),
      /** Uncompressed (i.e., may be split). */
      UNCOMPRESSED(""),
      /** GZipped. */
      GZIP(".gz"),
      /** BZipped. */
      BZIP2(".bz2"),
      /** Zipped. */
      ZIP(".zip"),
      /** Deflate compressed. */
      DEFLATE(".deflate");

      private String filenameSuffix;

      CompressionType(String suffix) {
        this.filenameSuffix = suffix;
      }

      /**
       * Determine if a given filename matches a compression type based on its extension.
       *
       * @param filename the filename to match
       * @return true iff the filename ends with the compression type's known extension.
       */
      public boolean matches(String filename) {
        return filename.toLowerCase().endsWith(filenameSuffix.toLowerCase());
      }
    }

    /**
     * Reads a single XML file or a set of XML files defined by a Java "glob" file pattern. Each XML
     * file should be of the form defined in {@link #read}.
     */
    public Read<T> from(String fileOrPatternSpec) {
      return toBuilder().setFileOrPatternSpec(fileOrPatternSpec).build();
    }

    /**
     * Sets name of the root element of the XML document. This will be used to create a valid
     * starting root element when initiating a bundle of records created from an XML document. This
     * is a required parameter.
     */
    public Read<T> withRootElement(String rootElement) {
      return toBuilder().setRootElement(rootElement).build();
    }

    /**
     * Sets name of the record element of the XML document. This will be used to determine offset of
     * the first record of a bundle created from the XML document. This is a required parameter.
     */
    public Read<T> withRecordElement(String recordElement) {
      return toBuilder().setRecordElement(recordElement).build();
    }

    /**
     * Sets a JAXB annotated class that can be populated using a record of the provided XML file.
     * This will be used when unmarshalling record objects from the XML file. This is a required
     * parameter.
     */
    public Read<T> withRecordClass(Class<T> recordClass) {
      return toBuilder().setRecordClass(recordClass).build();
    }

    /**
     * Sets a parameter {@code minBundleSize} for the minimum bundle size of the source. Please
     * refer to {@link OffsetBasedSource} for the definition of minBundleSize. This is an optional
     * parameter.
     */
    public Read<T> withMinBundleSize(long minBundleSize) {
      return toBuilder().setMinBundleSize(minBundleSize).build();
    }

    /**
     * Decompresses all input files using the specified compression type.
     *
     * <p>If no compression type is specified, the default is {@link CompressionType#AUTO}. In this
     * mode, the compression type of the file is determined by its extension. Supports .gz, .bz2,
     * .zip and .deflate compression.
     */
    public Read<T> withCompressionType(CompressionType compressionType) {
      return toBuilder().setCompressionType(compressionType).build();
    }

    /**
     * Sets the XML file charset.
     */
    public Read<T> withCharset(Charset charset) {
      return toBuilder().setCharset(charset.name()).build();
    }

    /**
     * Sets the {@link ValidationEventHandler} to use with JAXB. Calling this with a {@code null}
     * parameter will cause the JAXB unmarshaller event handler to be unspecified.
     */
    public Read<T> withValidationEventHandler(ValidationEventHandler validationEventHandler) {
      return toBuilder().setValidationEventHandler(validationEventHandler).build();
    }

    @Override
    public void validate(PipelineOptions options) {
      checkNotNull(
          getRootElement(),
          "rootElement is null. Use builder method withRootElement() to set this.");
      checkNotNull(
          getRecordElement(),
          "recordElement is null. Use builder method withRecordElement() to set this.");
      checkNotNull(
          getRecordClass(),
          "recordClass is null. Use builder method withRecordClass() to set this.");
      checkNotNull(
          getCharset(),
          "charset is null. Use builder method withCharset() to set this.");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder
          .addIfNotDefault(
              DisplayData.item("minBundleSize", getMinBundleSize())
                  .withLabel("Minimum Bundle Size"),
              1L)
          .add(DisplayData.item("filePattern", getFileOrPatternSpec()).withLabel("File Pattern"))
          .addIfNotNull(
              DisplayData.item("rootElement", getRootElement()).withLabel("XML Root Element"))
          .addIfNotNull(
              DisplayData.item("recordElement", getRecordElement()).withLabel("XML Record Element"))
          .addIfNotNull(
              DisplayData.item("recordClass", getRecordClass()).withLabel("XML Record Class"))
          .addIfNotNull(
              DisplayData.item("charset", getCharset()).withLabel("Charset"));
    }

    @VisibleForTesting
    BoundedSource<T> createSource() {
      XmlSource<T> source = new XmlSource<>(this);
      switch (getCompressionType()) {
        case UNCOMPRESSED:
          return source;
        case AUTO:
          return CompressedSource.from(source);
        case BZIP2:
          return CompressedSource.from(source)
              .withDecompression(CompressedSource.CompressionMode.BZIP2);
        case GZIP:
          return CompressedSource.from(source)
              .withDecompression(CompressedSource.CompressionMode.GZIP);
        case ZIP:
          return CompressedSource.from(source)
              .withDecompression(CompressedSource.CompressionMode.ZIP);
        case DEFLATE:
          return CompressedSource.from(source)
              .withDecompression(CompressedSource.CompressionMode.DEFLATE);
        default:
          throw new IllegalArgumentException("Unknown compression type: " + getCompressionType());
      }
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      return input.apply(org.apache.beam.sdk.io.Read.from(createSource()));
    }
  }

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {
    @Nullable
    abstract ValueProvider<ResourceId> getFilenamePrefix();

    @Nullable
    abstract Class<T> getRecordClass();

    @Nullable
    abstract String getRootElement();

    @Nullable
    abstract String getCharset();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setFilenamePrefix(ValueProvider<ResourceId> prefix);

      abstract Builder<T> setRecordClass(Class<T> recordClass);

      abstract Builder<T> setRootElement(String rootElement);

      abstract Builder<T> setCharset(String charset);

      abstract Write<T> build();
    }

    /**
     * Writes to files with the given path prefix.
     *
     * <p>Output files will have the name {@literal {filenamePrefix}-0000i-of-0000n.xml} where n is
     * the number of output bundles.
     */
    public Write<T> to(String filenamePrefix) {
      ResourceId resourceId = FileBasedSink.convertToFileResourceIfPossible(filenamePrefix);
      return toBuilder().setFilenamePrefix(StaticValueProvider.of(resourceId)).build();
    }

    /**
     * Writes objects of the given class mapped to XML elements using JAXB.
     *
     * <p>The specified class must be able to be used to create a JAXB context.
     */
    public Write<T> withRecordClass(Class<T> recordClass) {
      return toBuilder().setRecordClass(recordClass).build();
    }

    /** Sets the enclosing root element for the generated XML files. */
    public Write<T> withRootElement(String rootElement) {
      return toBuilder().setRootElement(rootElement).build();
    }

    /** Sets the charset used to write the file. */
    public Write<T> withCharset(Charset charset) {
      return toBuilder().setCharset(charset.name()).build();
    }

    @Override
    public void validate(PipelineOptions options) {
      checkNotNull(getRecordClass(), "Missing a class to bind to a JAXB context.");
      checkNotNull(getRootElement(), "Missing a root element name.");
      checkNotNull(getFilenamePrefix(), "Missing a filename to write to.");
      checkNotNull(getCharset(), "Missing charset");
      try {
        JAXBContext.newInstance(getRecordClass());
      } catch (JAXBException e) {
        throw new RuntimeException("Error binding classes to a JAXB Context.", e);
      }
    }

    @Override
    public PDone expand(PCollection<T> input) {
      input.apply(org.apache.beam.sdk.io.WriteFiles.to(createSink()));
      return PDone.in(input.getPipeline());
    }

    @VisibleForTesting
    XmlSink<T> createSink() {
      return new XmlSink<>(this);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      createSink().populateFileBasedDisplayData(builder);
      builder
          .addIfNotNull(
              DisplayData.item("rootElement", getRootElement()).withLabel("XML Root Element"))
          .addIfNotNull(
              DisplayData.item("recordClass", getRecordClass()).withLabel("XML Record Class"))
          .addIfNotNull(
              DisplayData.item("charset", getCharset()).withLabel("Charset"));
    }
  }
}
