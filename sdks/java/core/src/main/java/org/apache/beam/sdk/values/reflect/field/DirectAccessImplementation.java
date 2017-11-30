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

package org.apache.beam.sdk.values.reflect.field;

import java.lang.reflect.Field;
import net.bytebuddy.description.field.FieldDescription;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.matcher.ElementMatchers;

/**
 * Emits the bytecode for the implementation of {@link FieldValueGetter#get(Object)}.
 *
 * <p>ByteBuddy has a MethodCall.invoke(method).on(pojo) to forward method calls. But there
 * doesn't seem to be similar tools to get the fields, so we need this class to emit bytecode
 * directly.
 *
 * <p>See ByteBuddy documentation for details of how this works.
 */
public class DirectAccessImplementation implements Implementation {

  private Field field;

  public DirectAccessImplementation(Field field) {
    this.field = field;
  }

  @Override
  public InstrumentedType prepare(InstrumentedType instrumentedType) {
    return instrumentedType;
  }

  @Override
  public ByteCodeAppender appender(final Target implementationTarget) {
    return new ByteCodeAppender() {
      @Override
      public Size apply(
          MethodVisitor methodVisitor,
          Context implementationContext,
          MethodDescription instrumentedMethod) {

        FieldDescription accessedField = getFieldDescription(instrumentedMethod, field.getName());

        StackManipulation.Size size = generateAccessorBytecode(accessedField)
            .apply(methodVisitor, implementationContext);

        return new Size(size.getMaximalSize(), instrumentedMethod.getStackSize());
      }
    };
  }

  private FieldDescription getFieldDescription(
      MethodDescription instrumentedMethod,
      String fieldName) {

    return instrumentedMethod
        .getParameters()
        .asTypeList()
        .get(0)
        .getDeclaredFields()
        .filter(ElementMatchers.named(fieldName))
        .getOnly();
  }

  private StackManipulation.Compound generateAccessorBytecode(FieldDescription accessedField) {
    return new StackManipulation.Compound(
        MethodVariableAccess.REFERENCE.loadFrom(1), // load 1st arg of the FieldValueGetter.get()
        FieldAccess.forField(accessedField).read(), // read
        MethodReturn.REFERENCE); // return
  }
}

