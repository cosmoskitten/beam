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
package org.apache.beam.sdk.extensions.sql.impl.parser;

import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.utils.QuickCheckGenerators;
import org.apache.beam.sdk.extensions.sql.utils.QuickCheckGenerators.AnyFieldType;
import org.apache.beam.sdk.extensions.sql.utils.QuickCheckGenerators.PrimitiveTypes;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.calcite.sql.SqlNode;
import org.junit.runner.RunWith;

/**
 * Tests nested types using {@link JUnitQuickcheck}.
 *
 * <p>Types are randomly generated by {@link QuickCheckGenerators generators}.
 *
 * <p>By default quick check runs this test 100 times.
 */
@RunWith(JUnitQuickcheck.class)
public class BeamDDLNestedTypesTest {

  @Property
  public void supportsNestedTypes(@From(AnyFieldType.class) FieldType generatedFieldType) {
    String fieldTypeDeclaration = unparse(generatedFieldType);

    Table table = executeCreateTableWith(fieldTypeDeclaration);

    Schema expectedSchema = newSimpleSchemaWith(generatedFieldType);

    assertEquals(expectedSchema, table.getSchema());
  }

  @Property
  public void supportsPrimitiveTypes(@From(PrimitiveTypes.class) FieldType fieldType) {
    String fieldTypeDeclaration = unparse(fieldType);

    Table table = executeCreateTableWith(fieldTypeDeclaration);

    Schema expectedSchema = newSimpleSchemaWith(fieldType);

    assertEquals(expectedSchema, table.getSchema());
  }

  private Table executeCreateTableWith(String fieldType) {
    String createTable =
        "create table tablename ( "
        + "fieldName " + fieldType + " ) "
        + "TYPE 'text' "
        + "LOCATION '/home/admin/person'\n";
    System.out.println(createTable);

    SqlNode sqlNode;
    try {
      sqlNode = ParserTestUtils.parse(createTable);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertNotNull(sqlNode);
    assertTrue(sqlNode instanceof SqlCreateTable);
    SqlCreateTable stmt = (SqlCreateTable) sqlNode;
    return stmt.tableDefinition();
  }

  private Schema newSimpleSchemaWith(FieldType fieldType) {
    return Schema
        .builder()
        .addField(Field.of("fieldname", fieldType).withNullable(true))
        .build();
  }

  private String unparse(FieldType fieldType) {
    if (fieldType.getTypeName().isMapType()) {
      return unparseMap(fieldType);
    } else if (fieldType.getTypeName().isCollectionType()){
      return unparseArray(fieldType);
    } else if (fieldType.getTypeName().isCompositeType()) {
      return unparseRow(fieldType);
    } else {
      return unparsePrimitive(fieldType);
    }
  }

  private String unparsePrimitive(FieldType fieldType) {
    return CalciteUtils.toSqlTypeName(fieldType).getName();
  }

  private String unparseArray(FieldType fieldType) {
    return "ARRAY<" + unparse(fieldType.getCollectionElementType()) + ">";
  }

  private String unparseMap(FieldType fieldType) {
    return "MAP<"
           + unparse(fieldType.getMapKeyType())
           + ", "
           + unparse(fieldType.getMapValueType()) + ">";
  }

  private String unparseRow(FieldType fieldType) {
    return
        "ROW<"
        + fieldType
            .getRowSchema()
            .getFields()
            .stream()
            .map(field -> field.getName() + " " + unparse(field.getType()))
            .collect(joining(","))
        + ">";
  }
}
