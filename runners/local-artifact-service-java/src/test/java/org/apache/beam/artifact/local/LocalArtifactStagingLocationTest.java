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

package org.apache.beam.artifact.local;

import static com.google.common.base.Preconditions.checkState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link LocalArtifactStagingLocation}.
 */
@RunWith(JUnit4.class)
public class LocalArtifactStagingLocationTest {
  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void createAtWithAbsentDirectory() throws Exception {
    File baseFolder = tmp.newFolder();
    File root = new File(baseFolder, "foo");

    assertThat(root.exists(), is(false));
    LocalArtifactStagingLocation.createAt(root);

    assertThat(root.exists(), is(true));
    assertThat(root.listFiles().length, equalTo(1));
  }

  @Test
  public void createAtWithExistingDirectory() throws Exception {
    File baseFolder = tmp.newFolder();
    File root = new File(baseFolder, "foo");
    checkState(root.mkdir(), "Must be able to create the root directory");

    assertThat(root.exists(), is(true));
    assertThat(root.listFiles().length, equalTo(0));
    LocalArtifactStagingLocation.createAt(root);

    assertThat(root.exists(), is(true));
    assertThat(root.listFiles().length, equalTo(1));
  }

  @Test
  public void createAtWithUnwritableDirectory() throws Exception {
    File baseFolder = tmp.newFolder();
    File root = new File(baseFolder, "foo");
    checkState(root.mkdir(), "Must be able to create the root directory");

    assertThat(root.exists(), is(true));
    checkState(root.setWritable(false), "Must be able to set the root directory to unwritable");

    thrown.expect(IllegalStateException.class);
    LocalArtifactStagingLocation.createAt(root);
  }
}
