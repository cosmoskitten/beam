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
package org.apache.beam.runners.flink;

import static org.apache.beam.runners.core.construction.PipelineResources.detectClassPathResourcesToStage;

import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.util.common.ReflectHelpers;

class ClassLoaderUtils {

  static List<String> detectFilesToStageFromClass(Class<?> cls) {
    return Stream.concat(Stream.of(cls), parentClosure(ReflectHelpers.findClassLoader()))
        .filter(loader -> loader instanceof URLClassLoader)
        .flatMap(loader -> detectClassPathResourcesToStage(loader).stream())
        .distinct()
        .collect(Collectors.toList());
  }

  private static Stream<ClassLoader> parentClosure(ClassLoader loader) {
    List<ClassLoader> ret = new ArrayList<>();
    while (loader != null) {
      ret.add(loader);
      loader = loader.getParent();
    }
    return ret.stream();
  }

  private ClassLoaderUtils() {}
}
