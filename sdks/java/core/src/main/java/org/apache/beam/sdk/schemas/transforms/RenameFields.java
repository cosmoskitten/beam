package org.apache.beam.sdk.schemas.transforms;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Multimap;

public class RenameFields {
  public <T> Inner<T> rename(String field, String newName) {
    return new Inner<>(field, newName);
  }

  private static class RenamePair implements Serializable {
    private final FieldAccessDescriptor fieldAccessDescriptor;
    private final String newName;

    RenamePair(FieldAccessDescriptor fieldAccessDescriptor, String newName) {
      this.fieldAccessDescriptor = fieldAccessDescriptor;
      this.newName = newName;
    }
  }

  // Apply the user-specified renames to the input schema.
  private static FieldType rename(Schema inputSchema,  Collection<RenamePair> renames) {
    // The mapping of renames to apply at this level of the schema.
    Map<Integer, String> topLevelRenames = Maps.newHashMap();
    // For nested schemas, collect all applicable renames here.
    Multimap<Integer, RenamePair> nestedRenames = ArrayListMultimap.create();

    for (RenamePair rename : renames) {
      FieldAccessDescriptor access = rename.fieldAccessDescriptor;
      if (!access.referencesSingleField()) {
        throw new IllegalArgumentException(access + " references multiple fields.");
      }
      if (!access.fieldIdsAccessed().isEmpty()) {
        Integer fieldId = Iterables.getOnlyElement(access.fieldIdsAccessed());
        topLevelRenames.put(fieldId, rename.newName);
      } else {
        Map.Entry<Integer, FieldAccessDescriptor> nestedAccess =
            Iterables.getOnlyElement(access.nestedFieldsById().entrySet());
        FieldType nestedFieldType = inputSchema.getField(nestedAccess.getKey()).getType();
        Preconditions.checkArgument(nestedFieldType.getTypeName().isCompositeType());
        nestedRenames.put(nestedAccess.getKey(),
            new RenamePair(nestedAccess.getValue(), rename.newName));
      }
    }

    Schema.Builder builder = Schema.builder();
    for (int i = 0; i < inputSchema.getFieldCount(); ++i) {
      Field field = inputSchema.getField(i);
      FieldType fieldType = field.getType();
      String newName = topLevelRenames.get(i);
      if (newName != null) {
        builder.addField(newName, fieldType);
        continue;
      }

      Collection<RenamePair> nestedPairs = nestedRenames.asMap().get(i);
      if (nestedPairs != null) {
        // Recursively apply the rename to the subschema.
        Schema nestedSchema = getRenamedSchema(fieldType.getRowSchema(), nestedPairs);
        builder.addField(field.getName(),
            FieldType.row(nestedSchema).withNullable(field.getType().getNullable()));
      } else {
        builder.addField(field);
      }
    }
    return builder.build();
  }

  public class Inner<T> extends PTransform<PCollection<T>, PCollection<Row>> {
    private Map<FieldAccessDescriptor, String> renames;

    private Inner(String field, String newName) {
      renames.put(field, newName);
    }

    private Inner(Map<FieldAccessDescriptor, String> renames) {
      this.renames = renames;
    }

    public Inner<T> fieldAs(String field, String newName) {
      return fieldAs(FieldAccessDescriptor.withFieldNames(field), newName);
    }

    public Inner<T> fieldAs(FieldAccessDescriptor field, String newName) {
      Map<FieldAccessDescriptor, String> newMap =
              ImmutableMap.<FieldAccessDescriptor, String>builder()
                      .putAll(renames)
                      .put(field, newName)
                      .build();
      return new Inner<>(newMap);
    }

    @Override
    public PCollection<Row> expand(PCollection<T> input) {
      Schema inputSchema = input.getSchema();

      List<RenamePair> pairs = renames.entrySet().stream()
          .map(e -> new RenamePair(e.getKey().resolve(inputSchema), e.getValue()))
          .collect(Collectors.toList());
      final Schema outputSchema = getRenamedSchema(inputSchema, pairs);

      return input.apply(ParDo.of(new DoFn<T, Row>() {
        @ProcessElement
        public void processElement(@Element Row row, OutputReceiver<Row> o) {
          o.output(Row.withSchema(outputSchema).attachValues(row.getValues()).build());
        }
      })).setRowSchema(outputSchema);
    }
  }
}
