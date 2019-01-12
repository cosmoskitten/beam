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
package org.apache.beam.runners.dataflow.util;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.ServiceLoader;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.ModelCoderRegistrar;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;

/** Utilities for converting an object to a {@link CloudObject}. */
public class CloudObjects {
  private CloudObjects() {}

  static final Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>
      CODER_TRANSLATORS = populateCoderTranslators();
  static final Map<String, CloudObjectTranslator<? extends Coder>>
      CLOUD_OBJECT_CLASS_NAME_TRANSLATORS = populateCloudObjectTranslators();

  private static Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>
      populateCoderTranslators() {
    ImmutableMap.Builder<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>> builder =
        ImmutableMap.builder();
    for (CoderCloudObjectTranslatorRegistrar coderRegistrar :
        ServiceLoader.load(CoderCloudObjectTranslatorRegistrar.class)) {
      builder.putAll(coderRegistrar.classesToTranslators());
    }
    return builder.build();
  }

  private static Map<String, CloudObjectTranslator<? extends Coder>>
      populateCloudObjectTranslators() {
    ImmutableMap.Builder<String, CloudObjectTranslator<? extends Coder>> builder =
        ImmutableMap.builder();
    for (CoderCloudObjectTranslatorRegistrar coderRegistrar :
        ServiceLoader.load(CoderCloudObjectTranslatorRegistrar.class)) {
      builder.putAll(coderRegistrar.classNamesToTranslators());
    }
    return builder.build();
  }

  /** Convert the provided {@link Coder} into a {@link CloudObject}. */
  public static CloudObject asCloudObject(Coder<?> coder, @Nullable SdkComponents sdkComponents) {
    CloudObjectTranslator<Coder> translator =
        (CloudObjectTranslator<Coder>) CODER_TRANSLATORS.get(coder.getClass());
    CloudObject encoding;
    if (translator != null) {
      encoding = translator.toCloudObject(coder, sdkComponents);
    } else {
      CloudObjectTranslator customCoderTranslator = CODER_TRANSLATORS.get(CustomCoder.class);
      checkNotNull(
          customCoderTranslator,
          "No %s registered for %s, but it is in the %s",
          CloudObjectTranslator.class.getSimpleName(),
          CustomCoder.class.getSimpleName(),
          DefaultCoderCloudObjectTranslatorRegistrar.class.getSimpleName());
      encoding = customCoderTranslator.toCloudObject(coder, sdkComponents);
    }
    if (sdkComponents != null && !ModelCoderRegistrar.isKnownCoder(coder)) {
      try {
        String coderId = sdkComponents.registerCoder(coder);
        Structs.addString(encoding, PropertyNames.PIPELINE_PROTO_CODER_ID, coderId);
      } catch (Exception e) {
        throw new RuntimeException("Unable to register coder " + coder, e);
      }
    }
    return encoding;
  }

  public static Coder<?> coderFromCloudObject(CloudObject cloudObject) {
    CloudObjectTranslator<? extends Coder> translator =
        CLOUD_OBJECT_CLASS_NAME_TRANSLATORS.get(cloudObject.getClassName());
    checkArgument(
        translator != null,
        "Unknown %s class %s",
        Coder.class.getSimpleName(),
        cloudObject.getClassName());
    return translator.fromCloudObject(cloudObject);
  }
}
