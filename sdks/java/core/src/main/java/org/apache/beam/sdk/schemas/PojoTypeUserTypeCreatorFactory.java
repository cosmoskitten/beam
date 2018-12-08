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
package org.apache.beam.sdk.schemas;

import java.lang.reflect.Constructor;
import org.apache.beam.sdk.schemas.utils.FieldNamePolicies;
import org.apache.beam.sdk.schemas.utils.POJOUtils;

/** Vends constructors for POJOs. */
class PojoTypeUserTypeCreatorFactory implements UserTypeCreatorFactory {
  @Override
  public SchemaUserTypeCreator create(Class<?> clazz, Schema schema) {
    Constructor<?> constructor =
        POJOUtils.getConstructor(clazz, schema, FieldNamePolicies.identity());
    return new SchemaUserTypeConstructorCreator(clazz, constructor);
  }
}
