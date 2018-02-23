package org.apache.beam.runners.fnexecution.environment;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DockerWrapperTest {

  // TODO: How should we run integration tests that depend on docker? We should probably filter by
  // annotation so that unit tests don't automatically invoke docker on developers' machines.

  @Test
  public void helloWorld() throws Exception {
    DockerWrapper docker = getWrapper();
    String container = docker.runImage("hello-world", Collections.emptyList());
    System.out.printf("Started container: %s%n", container);
  }

  @Test
  public void killContainer() throws Exception {
    DockerWrapper docker = getWrapper();
    String container = docker.runImage("debian", Arrays.asList("/bin/bash", "-c", "sleep 60"));
    docker.killContainer(container);
  }

  private static DockerWrapper getWrapper() {
    return DockerWrapper.forCommand(
        "docker", Duration.ofMillis(10000));
  }

}
