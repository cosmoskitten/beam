package org.apache.beam.sdk.schemas.transforms;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.schemas.DefaultSchema;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class ConvertTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @DefaultSchema(JavaFieldSchema.class)
  public static class POJO1 {
    public String field1 = "field1";
    public long field2 = 42;
    public POJO1Nested field3 = new POJO1Nested();
    public POJO1Nested[] field4 = new POJO1Nested[] {new POJO1Nested(), new POJO1Nested()};
    public Map<String, POJO1Nested> field5 =
        ImmutableMap.of(
            "first", new POJO1Nested(),
            "second", new POJO1Nested());

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      POJO1 pojo1 = (POJO1) o;
      return field2 == pojo1.field2
          && Objects.equals(field1, pojo1.field1)
          && Objects.equals(field3, pojo1.field3)
          && Arrays.equals(field4, pojo1.field4)
          && Objects.equals(field5, pojo1.field5);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(field1, field2, field3, field5);
      result = 31 * result + Arrays.hashCode(field4);
      return result;
    }
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class POJO1Nested {
    public String yard1 = "yard2";
    public long yard2 = 43;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      POJO1Nested that = (POJO1Nested) o;
      return yard2 == that.yard2 && Objects.equals(yard1, that.yard1);
    }

    @Override
    public int hashCode() {
      return Objects.hash(yard1, yard2);
    }
  }

  public static final Schema EXPECTED_SCHEMA1_NESTED =
      Schema.builder().addStringField("yard1").addInt64Field("yard2").build();

  public static final Schema EXPECTED_SCHEMA1 =
      Schema.builder()
          .addStringField("field1")
          .addInt64Field("field2")
          .addRowField("field3", EXPECTED_SCHEMA1_NESTED)
          .addArrayField("field4", FieldType.row(EXPECTED_SCHEMA1_NESTED))
          .addMapField("field5", FieldType.STRING, FieldType.row(EXPECTED_SCHEMA1_NESTED))
          .build();

  public static final Row EXPECTED_ROW1_NESTED =
      Row.withSchema(EXPECTED_SCHEMA1_NESTED).addValues("yard2", 43L).build();
  public static final Row EXPECTED_ROW1 =
      Row.withSchema(EXPECTED_SCHEMA1)
          .addValue("field1")
          .addValue(42L)
          .addValue(EXPECTED_ROW1_NESTED)
          .addArray(ImmutableList.of(EXPECTED_ROW1_NESTED, EXPECTED_ROW1_NESTED))
          .addValue(ImmutableMap.of("first", EXPECTED_ROW1_NESTED, "second", EXPECTED_ROW1_NESTED))
          .build();

  // These classes will have different but equivalent (same fields in different order) schemas.
  @DefaultSchema(JavaFieldSchema.class)
  public static class POJO2 {
    public Map<String, POJO2Nested> field5 =
        ImmutableMap.of(
            "first", new POJO2Nested(),
            "second", new POJO2Nested());
    public POJO2Nested[] field4 = new POJO2Nested[] {new POJO2Nested(), new POJO2Nested()};
    public POJO2Nested field3 = new POJO2Nested();
    public long field2 = 42;
    public String field1 = "field1";

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      POJO2 pojo2 = (POJO2) o;
      return field2 == pojo2.field2
          && Objects.equals(field5, pojo2.field5)
          && Arrays.equals(field4, pojo2.field4)
          && Objects.equals(field3, pojo2.field3)
          && Objects.equals(field1, pojo2.field1);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(field5, field3, field2, field1);
      result = 31 * result + Arrays.hashCode(field4);
      return result;
    }
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class POJO2Nested {
    public long yard2 = 43;
    public String yard1 = "yard2";

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      POJO2Nested that = (POJO2Nested) o;
      return yard2 == that.yard2 && Objects.equals(yard1, that.yard1);
    }

    @Override
    public int hashCode() {

      return Objects.hash(yard2, yard1);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testToRows() {
    PCollection<Row> rows = pipeline.apply(Create.of(new POJO1())).apply(Convert.toRows());
    PAssert.that(rows).containsInAnyOrder(EXPECTED_ROW1);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFromRows() {
    PCollection<POJO1> pojos =
        pipeline
            .apply(
                Create.of(EXPECTED_ROW1)
                    .withSchema(
                        EXPECTED_SCHEMA1,
                        SerializableFunctions.identity(),
                        SerializableFunctions.identity()))
            .apply(Convert.fromRows(POJO1.class));
    PAssert.that(pojos).containsInAnyOrder(new POJO1());
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGeneralConvert() {
    PCollection<POJO2> pojos =
        pipeline.apply(Create.of(new POJO1())).apply(Convert.to(POJO2.class));
    PAssert.that(pojos).containsInAnyOrder(new POJO2());
    pipeline.run();
  }
}
