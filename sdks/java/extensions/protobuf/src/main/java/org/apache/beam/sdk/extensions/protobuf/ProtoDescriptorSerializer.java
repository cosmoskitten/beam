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

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class that makes a Descriptor class Serializable. This should be part of the ProtoBuf
 * library but for moving forward this is copied to the Beam code base.
 *
 * <p>Follow https://github.com/protocolbuffers/protobuf/issues/6101 for the state of the feature
 * request in ProtoBuf.
 */
class ProtoDescriptorSerializer {

  private static void visitFileDescriptorTree(Map map, Descriptors.FileDescriptor fileDescriptor) {
    if (!map.containsKey(fileDescriptor.getName())) {
      map.put(fileDescriptor.getName(), fileDescriptor);
      List<Descriptors.FileDescriptor> dependencies = fileDescriptor.getDependencies();
      dependencies.forEach(fd -> visitFileDescriptorTree(map, fd));
    }
  }

  private static FileDescriptorSet createSet(Descriptors.FileDescriptor file) {
    HashMap<String, Descriptors.FileDescriptor> fileDescriptorMap = new HashMap<>();
    visitFileDescriptorTree(fileDescriptorMap, file);
    FileDescriptorSet.Builder builder = FileDescriptorSet.newBuilder();
    fileDescriptorMap.values().forEach(fd -> builder.addFile(fd.toProto()));
    return builder.build();
  }

  private static Map<String, FileDescriptorProto> extractProtoMap(
      FileDescriptorSet fileDescriptorSet) {
    HashMap<String, FileDescriptorProto> map = new HashMap<>();
    fileDescriptorSet.getFileList().forEach(fdp -> map.put(fdp.getName(), fdp));
    return map;
  }

  private static Descriptors.FileDescriptor getFileDescriptor(
      String name, FileDescriptorSet fileDescriptorSet) {
    Map<String, FileDescriptorProto> inMap = extractProtoMap(fileDescriptorSet);
    Map<String, Descriptors.FileDescriptor> outMap = new HashMap<>();
    return convertToFileDescriptorMap(name, inMap, outMap);
  }

  private static Descriptors.FileDescriptor convertToFileDescriptorMap(
      String name,
      Map<String, FileDescriptorProto> inMap,
      Map<String, Descriptors.FileDescriptor> outMap) {
    if (outMap.containsKey(name)) {
      return outMap.get(name);
    }
    FileDescriptorProto fileDescriptorProto = inMap.get(name);
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

  /**
   * Write a descriptor onto the ObjectOutputStream.
   *
   * <p>It writes the name of the file containing the message descriptor, then the message
   * descriptor full name. Then the complete file descriptor set serialized.
   */
  public static void writeObject(ObjectOutputStream oos, Descriptors.Descriptor descriptor)
      throws IOException {
    oos.writeUTF(descriptor.getFile().getName());
    oos.writeUTF(descriptor.getName());

    byte[] buffer = createSet(descriptor.getFile()).toByteArray();
    oos.writeInt(buffer.length);
    oos.write(buffer);
  }

  /**
   * Read a descriptor from the ObjectInputStream.
   *
   * <p>It will read the full name of the file descriptor, then the name pf the message descriptor
   * name. Then the complete file set is read. The name is used to get the correct Descriptor from
   * the set and return that instance.
   */
  public static Descriptors.Descriptor readObject(ObjectInputStream ois) throws IOException {
    String fileName = ois.readUTF();
    String typeName = ois.readUTF();

    byte[] buffer = new byte[ois.readInt()];
    ois.readFully(buffer);
    FileDescriptorSet fileDescriptorProto = FileDescriptorSet.parseFrom(buffer);
    Descriptors.FileDescriptor fileDescriptor = getFileDescriptor(fileName, fileDescriptorProto);

    for (Descriptors.Descriptor descriptor : fileDescriptor.getMessageTypes()) {

      if (descriptor.getName().equals(typeName)) {
        return descriptor;
      }
    }
    return null;
  }
}
