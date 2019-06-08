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
package org.apache.beam.sdk.extensions.protobuf;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * ProtoDomain is a container class for Protobuf descriptors. By using a domain for all descriptors
 * that are related to each other the FileDescriptorSet needs to be serialized only once in the
 * graph.
 *
 * <p>Using a domain also grantees that all Descriptors have object equality, just like statically
 * compiled Proto classes Descriptors. A lot of Java code isn't used to the new DynamicMessages an
 * assume always Object equality. Because of this the domain class is immutable.
 *
 * <p>ProtoDomains aren't assumed to be used on with normal Message descriptors, only with
 * DynamicMessage descriptors.
 */
public final class ProtoDomain implements Serializable {
  public static final long serialVersionUID = 1L;
  private transient DescriptorProtos.FileDescriptorSet fileDescriptorSet;
  private transient int hashCode;

  private transient Map<String, Descriptors.FileDescriptor> fileDescriptorMap;

  ProtoDomain() {
    this(DescriptorProtos.FileDescriptorSet.newBuilder().build());
  }

  public ProtoDomain(DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
    this.fileDescriptorSet = fileDescriptorSet;
    hashCode = java.util.Arrays.hashCode(this.fileDescriptorSet.toByteArray());
    crosswire();
  }

  private static Map<String, DescriptorProtos.FileDescriptorProto> extractProtoMap(
      DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
    HashMap<String, DescriptorProtos.FileDescriptorProto> map = new HashMap<>();
    fileDescriptorSet.getFileList().forEach(fdp -> map.put(fdp.getName(), fdp));
    return map;
  }

  private static Descriptors.FileDescriptor convertToFileDescriptorMap(
      String name,
      Map<String, DescriptorProtos.FileDescriptorProto> inMap,
      Map<String, Descriptors.FileDescriptor> outMap) {
    if (outMap.containsKey(name)) {
      return outMap.get(name);
    }
    DescriptorProtos.FileDescriptorProto fileDescriptorProto = inMap.get(name);
    List<Descriptors.FileDescriptor> dependencies = new ArrayList<>();
    if (fileDescriptorProto.getDependencyCount() > 0) {
      fileDescriptorProto
          .getDependencyList()
          .forEach(
              dependencyName ->
                  dependencies.add(convertToFileDescriptorMap(dependencyName, inMap, outMap)));
    }
    try {
      Descriptors.FileDescriptor fileDescriptor =
          Descriptors.FileDescriptor.buildFrom(
              fileDescriptorProto, dependencies.toArray(new Descriptors.FileDescriptor[0]));
      outMap.put(name, fileDescriptor);
      return fileDescriptor;
    } catch (Descriptors.DescriptorValidationException e) {
      throw new RuntimeException(e);
    }
  }

  private static void visitFileDescriptorTree(Map map, Descriptors.FileDescriptor fileDescriptor) {
    if (!map.containsKey(fileDescriptor.getName())) {
      map.put(fileDescriptor.getName(), fileDescriptor);
      List<Descriptors.FileDescriptor> dependencies = fileDescriptor.getDependencies();
      dependencies.forEach(fd -> visitFileDescriptorTree(map, fd));
    }
  }

  public static ProtoDomain buildFrom(Descriptors.Descriptor descriptor) {
    return buildFrom(descriptor.getFile());
  }

  public static ProtoDomain buildFrom(Descriptors.FileDescriptor fileDescriptor) {
    HashMap<String, Descriptors.FileDescriptor> fileDescriptorMap = new HashMap<>();
    visitFileDescriptorTree(fileDescriptorMap, fileDescriptor);
    DescriptorProtos.FileDescriptorSet.Builder builder =
        DescriptorProtos.FileDescriptorSet.newBuilder();
    fileDescriptorMap.values().forEach(fd -> builder.addFile(fd.toProto()));
    return new ProtoDomain(builder.build());
  }

  private void crosswire() {
    HashMap<String, DescriptorProtos.FileDescriptorProto> map = new HashMap<>();
    fileDescriptorSet.getFileList().forEach(fdp -> map.put(fdp.getName(), fdp));

    Map<String, Descriptors.FileDescriptor> outMap = new HashMap<>();
    map.forEach((fileName, proto) -> convertToFileDescriptorMap(fileName, map, outMap));
    fileDescriptorMap = outMap;
  }

  private void writeObject(ObjectOutputStream oos) throws IOException {
    byte[] buffer = fileDescriptorSet.toByteArray();
    oos.writeInt(buffer.length);
    oos.write(buffer);
  }

  private void readObject(ObjectInputStream ois) throws IOException {
    byte[] buffer = new byte[ois.readInt()];
    ois.readFully(buffer);
    fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(buffer);
    hashCode = java.util.Arrays.hashCode(buffer);
    crosswire();
  }

  public Descriptors.FileDescriptor getFileDescriptor(String name) {
    return fileDescriptorMap.get(name);
  }

  public Descriptors.Descriptor getDescriptor(String fileName, String typeName) {
    Descriptors.FileDescriptor fileDescriptor = getFileDescriptor(fileName);
    for (Descriptors.Descriptor descriptor : fileDescriptor.getMessageTypes()) {

      if (descriptor.getName().equals(typeName)) {
        return descriptor;
      }
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ProtoDomain that = (ProtoDomain) o;
    return hashCode == that.hashCode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(hashCode);
  }

  public boolean contains(Descriptors.Descriptor descriptor) {
    return getDescriptor(descriptor.getFile().getFullName(), descriptor.getName()) != null;
  }
}
