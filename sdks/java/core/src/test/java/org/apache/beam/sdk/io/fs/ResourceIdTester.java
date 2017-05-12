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
package org.apache.beam.sdk.io.fs;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY;
import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_FILE;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.testing.EqualsTester;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.FileSystems;

/**
 * A utility to test {@link ResourceId} implementations.
 */
public final class ResourceIdTester {
  public enum ResourceIdOptions {};

  /**
   * Enforces that the {@link ResourceId} implementation of {@code baseDirectory} meets the
   * {@link ResourceId} spec.
   */
  public static void runResourceIdBattery(ResourceId baseDirectory, ResourceIdOptions... options) {
    checkArgument(
        baseDirectory.isDirectory(), "baseDirectory %s is not a directory", baseDirectory);

    List<ResourceId> allResourceIds = new ArrayList<>();
    allResourceIds.add(baseDirectory);

    // Validate operations with resolving child resources.
    validateResolvingIds(baseDirectory, allResourceIds);

    // Validate that individual resources meet the fairly restrictive spec we have.
    validateResourceIds(allResourceIds);
  }

  private static void validateResolvingIds(
      ResourceId baseDirectory, List<ResourceId> allResourceIds) {
    ResourceId file1 = baseDirectory.resolve("child1", RESOLVE_FILE);
    ResourceId file2 = baseDirectory.resolve("child2", RESOLVE_FILE);
    ResourceId file2a = baseDirectory.resolve("child2", RESOLVE_FILE);
    allResourceIds.add(file1);
    allResourceIds.add(file2);
    assertFalse("Resolved file isDirectory()", file1.isDirectory());
    assertFalse("Resolved file isDirectory()", file2.isDirectory());
    assertFalse("Resolved file isDirectory()", file2a.isDirectory());

    ResourceId dir1 = baseDirectory.resolve("child1", RESOLVE_DIRECTORY);
    ResourceId dir2 = baseDirectory.resolve("child2", RESOLVE_DIRECTORY);
    ResourceId dir2a = baseDirectory.resolve("child2", RESOLVE_DIRECTORY);
    assertTrue("Resolved directory isDirectory()", dir1.isDirectory());
    assertTrue("Resolved directory isDirectory()", dir2.isDirectory());
    assertTrue("Resolved directory isDirectory()", dir2a.isDirectory());
    allResourceIds.add(dir1);
    allResourceIds.add(dir2);

    // ResourceIds in equality groups.
    new EqualsTester()
        .addEqualityGroup(file1)
        .addEqualityGroup(file2, file2a)
        .addEqualityGroup(dir1, dir1.getCurrentDirectory())
        .addEqualityGroup(dir2, dir2a, dir2.getCurrentDirectory())
        .addEqualityGroup(baseDirectory, file1.getCurrentDirectory(), file2.getCurrentDirectory())
        .testEquals();

    // ResourceId toString() in equality groups.
    new EqualsTester()
        .addEqualityGroup(file1.toString())
        .addEqualityGroup(file2.toString(), file2a.toString())
        .addEqualityGroup(dir1.toString(), dir1.getCurrentDirectory().toString())
        .addEqualityGroup(dir2.toString(), dir2a.toString(), dir2.getCurrentDirectory().toString())
        .addEqualityGroup(
            baseDirectory.toString(),
            file1.getCurrentDirectory().toString(),
            file2.getCurrentDirectory().toString())
        .testEquals();

    // TODO: test resolving strings that need to be escaped (what is the spec?).
    //   Need options, likely.
    // TODO: test resolving from files (what is the spec?)
  }

  private static void validateResourceIds(List<ResourceId> resourceIds) {
    for (ResourceId resourceId : resourceIds) {
      // ResourceIds should equal themselves.
      assertThat("ResourceId equal to itself", resourceId, equalTo(resourceId));

      // ResourceIds should be clonable via FileSystems#matchNewResource.
      ResourceId cloned;
      if (resourceId.isDirectory()) {
        cloned = FileSystems.matchNewResource(resourceId.toString(), true /* isDirectory */);
      } else {
        cloned = FileSystems.matchNewResource(resourceId.toString(), false /* isDirectory */);
      }
      assertThat(
          "ResourceId equals clone of itself", cloned, equalTo(resourceId));
      // .. and clones have consistent toString.
      assertThat(
          "ResourceId toString consistency", cloned.toString(), equalTo(resourceId.toString()));
      // .. and have consistent isDirectory.
      assertThat(
          "ResourceId isDirectory consistency",
          cloned.isDirectory(),
          equalTo(resourceId.isDirectory()));
    }
  }

  private ResourceIdTester() {} // prevent instantiation
}
