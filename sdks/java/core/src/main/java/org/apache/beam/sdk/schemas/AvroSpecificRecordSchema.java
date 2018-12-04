package org.apache.beam.sdk.schemas;

import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.schemas.utils.JavaBeanGetterFactory;
import org.apache.beam.sdk.schemas.utils.JavaBeanSetterFactory;
import org.apache.beam.sdk.schemas.utils.JavaBeanTypeInformationFactory;
import org.apache.beam.sdk.schemas.utils.JavaBeanUtils;
import org.apache.beam.sdk.values.TypeDescriptor;

public class AvroSpecificRecordSchema extends GetterBasedSchemaProvider {
  @Override
  public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    return AvroUtils.getSchema((Class<? extends SpecificRecord>) typeDescriptor.getRawType());
  }

  @Override
  public FieldValueGetterFactory fieldValueGetterFactory() {
   return new AvroSpecificRecordGetterFactory();
  }

  @Override
  public FieldValueSetterFactory fieldValueSetterFactory() {
  //  return new AvroSpecificRecordSetterFactory();
  }

  @Override
  public FieldValueTypeInformationFactory fieldValueTypeInformationFactory() {
 //   return new AvroSpecificRecordTypeInformationFactory();
  }
}
