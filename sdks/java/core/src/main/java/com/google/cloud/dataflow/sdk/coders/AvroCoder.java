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
package com.google.cloud.dataflow.sdk.coders;

import static com.google.cloud.dataflow.sdk.util.Structs.addString;

import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.AvroEncode;
import org.apache.avro.reflect.AvroName;
import org.apache.avro.reflect.AvroSchema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.reflect.Union;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.ClassUtils;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

import javax.annotation.Nullable;

/**
 * A {@link Coder} using Avro binary format.
 *
 * <p>Each instance of {@code AvroCoder<T>} encapsulates an Avro schema for objects of type
 * {@code T}.
 *
 * <p>The Avro schema may be provided explicitly via {@link AvroCoder#of(Class, Schema)} or
 * omitted via {@link AvroCoder#of(Class)}, in which case it will be inferred
 * using Avro's {@link org.apache.avro.reflect.ReflectData}.
 *
 * <p>For complete details about schema generation and how it can be controlled please see
 * the {@link org.apache.avro.reflect} package.
 * Only concrete classes with a no-argument constructor can be mapped to Avro records.
 * All inherited fields that are not static or transient are included. Fields are not permitted to
 * be null unless annotated by {@link Nullable} or a {@link Union} schema
 * containing {@code "null"}.
 *
 * <p>To use, specify the {@code Coder} type on a PCollection:
 * <pre>
 * {@code
 * PCollection<MyCustomElement> records =
 *     input.apply(...)
 *          .setCoder(AvroCoder.of(MyCustomElement.class);
 * }
 * </pre>
 *
 * <p>or annotate the element class using {@code @DefaultCoder}.
 * <pre><code>
 * {@literal @}DefaultCoder(AvroCoder.class)
 * public class MyCustomElement {
 *   ...
 * }
 * </code></pre>
 *
 * <p>The implementation attempts to determine if the Avro encoding of the given type will satisfy
 * the criteria of {@link Coder#verifyDeterministic} by inspecting both the type and the
 * Schema provided or generated by Avro. Only coders that are deterministic can be used in
 * {@link com.google.cloud.dataflow.sdk.transforms.GroupByKey} operations.
 *
 * @param <T> the type of elements handled by this coder
 */
public class AvroCoder<T> extends StandardCoder<T> {

  /**
   * Returns an {@code AvroCoder} instance for the provided element type.
   * @param <T> the element type
   */
  public static <T> AvroCoder<T> of(TypeDescriptor<T> type) {
    @SuppressWarnings("unchecked")
    Class<T> clazz = (Class<T>) type.getRawType();
    return of(clazz);
  }

  /**
   * Returns an {@code AvroCoder} instance for the provided element class.
   * @param <T> the element type
   */
  public static <T> AvroCoder<T> of(Class<T> clazz) {
    return new AvroCoder<>(clazz, ReflectData.get().getSchema(clazz));
  }

  /**
   * Returns an {@code AvroCoder} instance for the Avro schema. The implicit
   * type is GenericRecord.
   */
  public static AvroCoder<GenericRecord> of(Schema schema) {
    return new AvroCoder<>(GenericRecord.class, schema);
  }

  /**
   * Returns an {@code AvroCoder} instance for the provided element type
   * using the provided Avro schema.
   *
   * <p>If the type argument is GenericRecord, the schema may be arbitrary.
   * Otherwise, the schema must correspond to the type provided.
   *
   * @param <T> the element type
   */
  public static <T> AvroCoder<T> of(Class<T> type, Schema schema) {
    return new AvroCoder<>(type, schema);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @JsonCreator
  public static AvroCoder<?> of(
      @JsonProperty("type") String classType,
      @JsonProperty("schema") String schema) throws ClassNotFoundException {
    Schema.Parser parser = new Schema.Parser();
    return new AvroCoder(Class.forName(classType), parser.parse(schema));
  }

  public static final CoderProvider PROVIDER = new CoderProvider() {
    @Override
    public <T> Coder<T> getCoder(TypeDescriptor<T> typeDescriptor) {
      // This is a downcast from `? super T` to T. However, because
      // it comes from a TypeDescriptor<T>, the class object itself
      // is the same so the supertype in question shares the same
      // generated AvroCoder schema.
      @SuppressWarnings("unchecked")
      Class<T> rawType = (Class<T>) typeDescriptor.getRawType();
      return AvroCoder.of(rawType);
    }
  };

  private final Class<T> type;
  private final Schema schema;

  private final List<String> nonDeterministicReasons;

  // Factories allocated by .get() are thread-safe and immutable.
  private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();
  private static final DecoderFactory DECODER_FACTORY = DecoderFactory.get();
  // Cache the old encoder/decoder and let the factories reuse them when possible. To be threadsafe,
  // these are ThreadLocal. This code does not need to be re-entrant as AvroCoder does not use
  // an inner coder.
  private final ThreadLocal<BinaryDecoder> decoder;
  private final ThreadLocal<BinaryEncoder> encoder;
  private final ThreadLocal<DatumWriter<T>> writer;
  private final ThreadLocal<DatumReader<T>> reader;

  protected AvroCoder(Class<T> type, Schema schema) {
    this.type = type;
    this.schema = schema;

    nonDeterministicReasons = new AvroDeterminismChecker().check(TypeDescriptor.of(type), schema);

    // Decoder and Encoder start off null for each thread. They are allocated and potentially
    // reused inside encode/decode.
    this.decoder = new ThreadLocal<>();
    this.encoder = new ThreadLocal<>();

    // Reader and writer are allocated once per thread and are "final" for thread-local Coder
    // instance.
    this.reader = new ThreadLocal<DatumReader<T>>() {
      @Override
      public DatumReader<T> initialValue() {
        return createDatumReader();
      }
    };
    this.writer = new ThreadLocal<DatumWriter<T>>() {
      @Override
      public DatumWriter<T> initialValue() {
        return createDatumWriter();
      }
    };
  }

  /**
   * The encoding identifier is designed to support evolution as per the design of Avro
   * In order to use this class effectively, carefully read the Avro
   * documentation at
   * <a href="https://avro.apache.org/docs/1.7.7/spec.html#Schema+Resolution">Schema Resolution</a>
   * to ensure that the old and new schema <i>match</i>.
   *
   * <p>In particular, this encoding identifier is guaranteed to be the same for {@code AvroCoder}
   * instances of the same principal class, and otherwise distinct. The schema is not included
   * in the identifier.
   *
   * <p>When modifying a class to be encoded as Avro, here are some guidelines; see the above link
   * for greater detail.
   *
   * <ul>
   * <li>Avoid changing field names.
   * <li>Never remove a <code>required</code> field.
   * <li>Only add <code>optional</code> fields, with sensible defaults.
   * <li>When changing the type of a field, consult the Avro documentation to ensure the new and
   * old types are interchangeable.
   * </ul>
   *
   * <p>Code consuming this message class should be prepared to support <i>all</i> versions of
   * the class until it is certain that no remaining serialized instances exist.
   *
   * <p>If backwards incompatible changes must be made, the best recourse is to change the name
   * of your class.
   */
  @Override
  public String getEncodingId() {
    return type.getName();
  }

  /**
   * Returns the type this coder encodes/decodes.
   */
  public Class<T> getType() {
    return type;
  }

  private Object writeReplace() {
    // When serialized by Java, instances of AvroCoder should be replaced by
    // a SerializedAvroCoderProxy.
    return new SerializedAvroCoderProxy<>(type, schema.toString());
  }

  @Override
  public void encode(T value, OutputStream outStream, Context context) throws IOException {
    // Get a BinaryEncoder instance from the ThreadLocal cache and attempt to reuse it.
    BinaryEncoder encoderInstance = ENCODER_FACTORY.directBinaryEncoder(outStream, encoder.get());
    // Save the potentially-new instance for reuse later.
    encoder.set(encoderInstance);
    writer.get().write(value, encoderInstance);
    // Direct binary encoder does not buffer any data and need not be flushed.
  }

  @Override
  public T decode(InputStream inStream, Context context) throws IOException {
    // Get a BinaryDecoder instance from the ThreadLocal cache and attempt to reuse it.
    BinaryDecoder decoderInstance = DECODER_FACTORY.directBinaryDecoder(inStream, decoder.get());
    // Save the potentially-new instance for later.
    decoder.set(decoderInstance);
    return reader.get().read(null, decoderInstance);
  }

  @Override
    public List<? extends Coder<?>> getCoderArguments() {
    return null;
  }

  @Override
  public CloudObject asCloudObject() {
    CloudObject result = super.asCloudObject();
    addString(result, "type", type.getName());
    addString(result, "schema", schema.toString());
    return result;
  }

  /**
   * @throws NonDeterministicException when the type may not be deterministically
   * encoded using the given {@link Schema}, the {@code directBinaryEncoder}, and the
   * {@link ReflectDatumWriter} or {@link GenericDatumWriter}.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    if (!nonDeterministicReasons.isEmpty()) {
      throw new NonDeterministicException(this, nonDeterministicReasons);
    }
  }

  /**
   * Returns a new {@link DatumReader} that can be used to read from an Avro file directly. Assumes
   * the schema used to read is the same as the schema that was used when writing.
   *
   * @deprecated For {@code AvroCoder} internal use only.
   */
  // TODO: once we can remove this deprecated function, inline in constructor.
  @Deprecated
  public DatumReader<T> createDatumReader() {
    if (type.equals(GenericRecord.class)) {
      return new GenericDatumReader<>(schema);
    } else {
      return new ReflectDatumReader<>(schema);
    }
  }

  /**
   * Returns a new {@link DatumWriter} that can be used to write to an Avro file directly.
   *
   * @deprecated For {@code AvroCoder} internal use only.
   */
  // TODO: once we can remove this deprecated function, inline in constructor.
  @Deprecated
  public DatumWriter<T> createDatumWriter() {
    if (type.equals(GenericRecord.class)) {
      return new GenericDatumWriter<>(schema);
    } else {
      return new ReflectDatumWriter<>(schema);
    }
  }

  /**
   * Returns the schema used by this coder.
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Proxy to use in place of serializing the {@link AvroCoder}. This allows the fields
   * to remain final.
   */
  private static class SerializedAvroCoderProxy<T> implements Serializable {
    private final Class<T> type;
    private final String schemaStr;

    public SerializedAvroCoderProxy(Class<T> type, String schemaStr) {
      this.type = type;
      this.schemaStr = schemaStr;
    }

    private Object readResolve() {
      // When deserialized, instances of this object should be replaced by
      // constructing an AvroCoder.
      Schema.Parser parser = new Schema.Parser();
      return new AvroCoder<T>(type, parser.parse(schemaStr));
    }
  }

  /**
   * Helper class encapsulating the various pieces of state maintained by the
   * recursive walk used for checking if the encoding will be deterministic.
   */
  private static class AvroDeterminismChecker {

    // Reasons that the original type are not deterministic. This accumulates
    // the actual output.
    private List<String> reasons = new ArrayList<>();

    // Types that are currently "open". Used to make sure we don't have any
    // recursive types. Note that we assume that all occurrences of a given type
    // are equal, rather than tracking pairs of type + schema.
    private Set<TypeDescriptor<?>> activeTypes = new HashSet<>();

    // Similarly to how we record active types, we record the schemas we visit
    // to make sure we don't encounter recursive fields.
    private Set<Schema> activeSchemas = new HashSet<>();

    /**
     * Report an error in the current context.
     */
    private void reportError(String context, String fmt, Object... args) {
      String message = String.format(fmt, args);
      reasons.add(context + ": " + message);
    }

    /**
     * Classes that are serialized by Avro as a String include
     * <ul>
     * <li>Subtypes of CharSequence (including String, Avro's mutable Utf8, etc.)
     * <li>Several predefined classes (BigDecimal, BigInteger, URI, URL)
     * <li>Classes annotated with @Stringable (uses their #toString() and a String constructor)
     * </ul>
     *
     * <p>Rather than determine which of these cases are deterministic, we list some classes
     * that definitely are, and treat any others as non-deterministic.
     */
    private static final Set<Class<?>> DETERMINISTIC_STRINGABLE_CLASSES = new HashSet<>();
    static {
      // CharSequences:
      DETERMINISTIC_STRINGABLE_CLASSES.add(String.class);
      DETERMINISTIC_STRINGABLE_CLASSES.add(Utf8.class);

      // Explicitly Stringable:
      DETERMINISTIC_STRINGABLE_CLASSES.add(java.math.BigDecimal.class);
      DETERMINISTIC_STRINGABLE_CLASSES.add(java.math.BigInteger.class);
      DETERMINISTIC_STRINGABLE_CLASSES.add(java.net.URI.class);
      DETERMINISTIC_STRINGABLE_CLASSES.add(java.net.URL.class);

      // Classes annotated with @Stringable:
    }

    /**
     * Return true if the given type token is a subtype of *any* of the listed parents.
     */
    private static boolean isSubtypeOf(TypeDescriptor<?> type, Class<?>... parents) {
      for (Class<?> parent : parents) {
        if (type.isSubtypeOf(TypeDescriptor.of(parent))) {
          return true;
        }
      }
      return false;
    }

    protected AvroDeterminismChecker() {}

    // The entry point for the check. Should not be recursively called.
    public List<String> check(TypeDescriptor<?> type, Schema schema) {
      recurse(type.getRawType().getName(), type, schema);
      return reasons;
    }

    // This is the method that should be recursively called. It sets up the path
    // and visited types correctly.
    private void recurse(String context, TypeDescriptor<?> type, Schema schema) {
      if (type.getRawType().isAnnotationPresent(AvroSchema.class)) {
        reportError(context, "Custom schemas are not supported -- remove @AvroSchema.");
        return;
      }

      if (!activeTypes.add(type)) {
        reportError(context, "%s appears recursively", type);
        return;
      }

      // If the the record isn't a true class, but rather a GenericRecord, SpecificRecord, etc.
      // with a specified schema, then we need to make the decision based on the generated
      // implementations.
      if (isSubtypeOf(type, IndexedRecord.class)) {
        checkIndexedRecord(context, schema, null);
      } else {
        doCheck(context, type, schema);
      }

      activeTypes.remove(type);
    }

    private void doCheck(String context, TypeDescriptor<?> type, Schema schema) {
      switch (schema.getType()) {
        case ARRAY:
          checkArray(context, type, schema);
          break;
        case ENUM:
          // Enums should be deterministic, since they depend only on the ordinal.
          break;
        case FIXED:
          // Depending on the implementation of GenericFixed, we don't know how
          // the given field will be encoded. So, we assume that it isn't
          // deterministic.
          reportError(context, "FIXED encodings are not guaranteed to be deterministic");
          break;
        case MAP:
          checkMap(context, type, schema);
          break;
        case RECORD:
          checkRecord(type, schema);
          break;
        case UNION:
          checkUnion(context, type, schema);
          break;
        case STRING:
          checkString(context, type);
          break;
        case BOOLEAN:
        case BYTES:
        case DOUBLE:
        case INT:
        case FLOAT:
        case LONG:
        case NULL:
          // For types that Avro encodes using one of the above primitives, we assume they are
          // deterministic.
          break;
        default:
          // In any other case (eg., new types added to Avro) we cautiously return
          // false.
          reportError(context, "Unknown schema type %s may be non-deterministic", schema.getType());
          break;
      }
    }

    private void checkString(String context, TypeDescriptor<?> type) {
      // For types that are encoded as strings, we need to make sure they're in an approved
      // whitelist. For other types that are annotated @Stringable, Avro will just use the
      // #toString() methods, which has no guarantees of determinism.
      if (!DETERMINISTIC_STRINGABLE_CLASSES.contains(type.getRawType())) {
        reportError(context, "%s may not have deterministic #toString()", type);
      }
    }

   private static final Schema AVRO_NULL_SCHEMA = Schema.create(Schema.Type.NULL);

   private void checkUnion(String context, TypeDescriptor<?> type, Schema schema) {
      final List<Schema> unionTypes = schema.getTypes();

      if (!type.getRawType().isAnnotationPresent(Union.class)) {
        // First check for @Nullable field, which shows up as a union of field type and null.
        if (unionTypes.size() == 2 && unionTypes.contains(AVRO_NULL_SCHEMA)) {
          // Find the Schema that is not NULL and recursively check that it is deterministic.
          Schema nullableFieldSchema = unionTypes.get(0).equals(AVRO_NULL_SCHEMA)
              ? unionTypes.get(1) : unionTypes.get(0);
          doCheck(context, type, nullableFieldSchema);
          return;
        }

        // Otherwise report a schema error.
        reportError(context, "Expected type %s to have @Union annotation", type);
        return;
      }

      // Errors associated with this union will use the base class as their context.
      String baseClassContext = type.getRawType().getName();

      // For a union, we need to make sure that each possible instantiation is deterministic.
      for (Schema concrete : unionTypes) {
        @SuppressWarnings("unchecked")
        TypeDescriptor<?> unionType = TypeDescriptor.of(ReflectData.get().getClass(concrete));

        recurse(baseClassContext, unionType, concrete);
      }
    }

    private void checkRecord(TypeDescriptor<?> type, Schema schema) {
      // For a record, we want to make sure that all the fields are deterministic.
      Class<?> clazz = type.getRawType();
      for (org.apache.avro.Schema.Field fieldSchema : schema.getFields()) {
        Field field = getField(clazz, fieldSchema.name());
        String fieldContext = field.getDeclaringClass().getName() + "#" + field.getName();

        if (field.isAnnotationPresent(AvroEncode.class)) {
          reportError(fieldContext,
              "Custom encoders may be non-deterministic -- remove @AvroEncode");
          continue;
        }

        if (!IndexedRecord.class.isAssignableFrom(field.getType())
            && field.isAnnotationPresent(AvroSchema.class)) {
          // TODO: We should be able to support custom schemas on POJO fields, but we shouldn't
          // need to, so we just allow it in the case of IndexedRecords.
          reportError(fieldContext,
              "Custom schemas are only supported for subtypes of IndexedRecord.");
          continue;
        }

        TypeDescriptor<?> fieldType = type.resolveType(field.getGenericType());
        recurse(fieldContext, fieldType, fieldSchema.schema());
      }
    }

    private void checkIndexedRecord(String context, Schema schema,
        @Nullable String specificClassStr) {

      if (!activeSchemas.add(schema)) {
        reportError(context, "%s appears recursively", schema.getName());
        return;
      }

      switch (schema.getType()) {
        case ARRAY:
          // Generic Records use GenericData.Array to implement arrays, which is
          // essentially an ArrayList, and therefore ordering is deterministic.
          // The array is thus deterministic if the elements are deterministic.
          checkIndexedRecord(context, schema.getElementType(), null);
          break;
        case ENUM:
          // Enums are deterministic because they encode as a single integer.
          break;
        case FIXED:
          // In the case of GenericRecords, FIXED is deterministic because it
          // encodes/decodes as a Byte[].
          break;
        case MAP:
          reportError(context,
              "GenericRecord and SpecificRecords use a HashMap to represent MAPs,"
              + " so it is non-deterministic");
          break;
        case RECORD:
          for (org.apache.avro.Schema.Field field : schema.getFields()) {
            checkIndexedRecord(
                schema.getName() + "." + field.name(),
                field.schema(),
                field.getProp(SpecificData.CLASS_PROP));
          }
          break;
        case STRING:
          // GenericDatumWriter#findStringClass will use a CharSequence or a String
          // for each string, so it is deterministic.

          // SpecificCompiler#getStringType will use java.lang.String, org.apache.avro.util.Utf8,
          // or java.lang.CharSequence, unless SpecificData.CLASS_PROP overrides that.
          if (specificClassStr != null) {
            Class<?> specificClass;
            try {
              specificClass = ClassUtils.forName(specificClassStr);
              if (!DETERMINISTIC_STRINGABLE_CLASSES.contains(specificClass)) {
                reportError(context, "Specific class %s is not known to be deterministic",
                    specificClassStr);
              }
            } catch (ClassNotFoundException e) {
              reportError(context, "Specific class %s is not known to be deterministic",
                  specificClassStr);
            }
          }
          break;
        case UNION:
          for (org.apache.avro.Schema subschema : schema.getTypes()) {
            checkIndexedRecord(subschema.getName(), subschema, null);
          }
          break;
        case BOOLEAN:
        case BYTES:
        case DOUBLE:
        case INT:
        case FLOAT:
        case LONG:
        case NULL:
          // For types that Avro encodes using one of the above primitives, we assume they are
          // deterministic.
          break;
        default:
          reportError(context, "Unknown schema type %s may be non-deterministic", schema.getType());
          break;
      }

      activeSchemas.remove(schema);
    }

    private void checkMap(String context, TypeDescriptor<?> type, Schema schema) {
      if (!isSubtypeOf(type, SortedMap.class)) {
        reportError(context, "%s may not be deterministically ordered", type);
      }

      // Avro (currently) asserts that all keys are strings.
      // In case that changes, we double check that the key was a string:
      Class<?> keyType = type.resolveType(Map.class.getTypeParameters()[0]).getRawType();
      if (!String.class.equals(keyType)) {
        reportError(context, "map keys should be Strings, but was %s", keyType);
      }

      recurse(context,
          type.resolveType(Map.class.getTypeParameters()[1]),
          schema.getValueType());
    }

    private void checkArray(String context, TypeDescriptor<?> type, Schema schema) {
      TypeDescriptor<?> elementType = null;
      if (type.isArray()) {
        // The type is an array (with ordering)-> deterministic iff the element is deterministic.
        elementType = type.getComponentType();
      } else if (isSubtypeOf(type, Collection.class)) {
        if (isSubtypeOf(type, List.class, SortedSet.class)) {
          // Ordered collection -> deterministic iff the element is deterministic
          elementType = type.resolveType(Collection.class.getTypeParameters()[0]);
        } else {
          // Not an ordered collection -> not deterministic
          reportError(context, "%s may not be deterministically ordered", type);
          return;
        }
      } else {
        // If it was an unknown type encoded as an array, be conservative and assume
        // that we don't know anything about the order.
        reportError(context, "encoding %s as an ARRAY was unexpected");
        return;
      }

      // If we get here, it's either a deterministically-ordered Collection, or
      // an array. Either way, the type is deterministic iff the element type is
      // deterministic.
      recurse(context, elementType, schema.getElementType());
    }

    /**
     * Extract a field from a class. We need to look at the declared fields so that we can
     * see private fields. We may need to walk up to the parent to get classes from the parent.
     */
    private static Field getField(Class<?> clazz, String name) {
      while (clazz != null) {
        for (Field field : clazz.getDeclaredFields()) {
          AvroName avroName = field.getAnnotation(AvroName.class);
          if (avroName != null && name.equals(avroName.value())) {
            return field;
          } else if (avroName == null && name.equals(field.getName())) {
            return field;
          }
        }
        clazz = clazz.getSuperclass();
      }

      throw new IllegalArgumentException(
          "Unable to get field " + name + " from class " + clazz);
    }
  }
}
