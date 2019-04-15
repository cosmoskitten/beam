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
package org.apache.beam.sdk.io.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.auto.service.AutoService;
import java.util.List;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.values.TypeDescriptor;

// TODO: Question: do we still need this Provider?
/** TODO: Doc it. */
@AutoService(CoderProvider.class)
public class AttributeValueCoderProvider extends CoderProvider {
  @Override
  public <T> Coder<T> coderFor(
      TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders)
      throws CannotProvideCoderException {
    if (!AttributeValue.class.isAssignableFrom(typeDescriptor.getRawType())) {
      throw new CannotProvideCoderException(
          String.format("Type %s is not a AttributeValueCoder", typeDescriptor));
    }

    @SuppressWarnings("unchecked")
    final Coder<T> coder = (Coder<T>) AttributeValueCoder.of();
    return coder;
  }
}
