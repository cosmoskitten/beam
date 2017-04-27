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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.Structs;

/** A {@link CloudObjectTranslator} for {@link ProtoCoder}. */
public class ProtoCoderCloudObjectTranslator implements CloudObjectTranslator<ProtoCoder> {
  private static final String PROTO_MESSAGE_CLASS = "proto_message_class";
  private static final String PROTO_EXTENSION_HOSTS = "proto_extension_hosts";

  @Override
  public CloudObject toCloudObject(ProtoCoder target) {
    CloudObject base = CloudObject.forClass(ProtoCoder.class);
    Structs.addString(base, PROTO_MESSAGE_CLASS, target.getMessageType().getName());
    Set<Class<?>> extensionHosts = target.getExtensionHosts();
    List<String> extensionHostNames = new ArrayList<>();
    for (Class<?> extensionHost : extensionHosts) {
      extensionHostNames.add(extensionHost.getName());
    }
    Structs.addStringList(base, PROTO_EXTENSION_HOSTS, extensionHostNames);
    return base;
  }

  @Override
  public ProtoCoder<?> fromCloudObject(CloudObject cloudObject) {
    String protoMessageClassName = Structs.getString(cloudObject, PROTO_MESSAGE_CLASS);
    List<String> extensionHostClassNames =
        Structs.getStrings(cloudObject, PROTO_EXTENSION_HOSTS, Collections.<String>emptyList());
    try {
      Class<? extends Message> protoMessageClass =
          (Class<? extends Message>) Class.forName(protoMessageClassName);
      checkArgument(
          Message.class.isAssignableFrom(protoMessageClass),
          "Target Class %s does not extend %s",
          protoMessageClass.getName(),
          Message.class.getSimpleName());
      List<Class<?>> extensionHostClasses = new ArrayList<>();
      for (String extensionHostClassName : extensionHostClassNames) {
        extensionHostClasses.add(Class.forName(extensionHostClassName));
      }
      return ProtoCoder.of(protoMessageClass).withExtensionsFrom(extensionHostClasses);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public Class<? extends ProtoCoder> getSupportedClass() {
    return ProtoCoder.class;
  }

  @Override
  public String cloudObjectClassName() {
    return CloudObject.forClass(ProtoCoder.class).getClassName();
  }
}
