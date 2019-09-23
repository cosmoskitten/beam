---
layout: section
title: "Runtime environments"
section_menu: section-menu/documentation.html
permalink: /documentation/runtime/environments
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Runtime environments

Any execution engine can run the Beam SDK beacuse the SDK runtime environment is [containerized](https://s.apache.org/beam-fn-api-container-contract) with [Docker](https://www.docker.com/) and isolated from other runtime systems. This page describes how to build, customize, and push Beam SDK container images.

## Building container images

Before building Beam SDK container images:
* Register a [Bintray](https://bintray.com/) account with a Docker repository named `apache`.
* Install [Docker](https://www.docker.com/) on your workstation.

To build Beam SDK container images:

<ol>
    <li>
        Navigate to your local copy of the <a href="https://github.com/apache/beam"><code>beam</code></a>
    </li>
    <li>
        Run Gradle with the <code>docker</code> target: <pre>./gradlew docker</pre>
    </li>
</ol>

> **Note**: It may take a long time to build all of the container images. You can instead build the images for specific SDKs:
>
> ```
> ./gradlew -p sdks/java/container docker
> ./gradlew -p sdks/python/container docker
> ./gradlew -p sdks/go/container docker
> ```

To examine the SDK runtime environments, run the `docker images` command. For example, if you successfully built the container images, the command prompt displays a response like:

```
REPOSITORY                                       TAG                    IMAGE ID            CREATED       SIZE
$USER-docker-apache.bintray.io/beam/python     latest             4ea515403a1a      3 minutes ago     1.27GB
$USER-docker-apache.bintray.io/beam/java       latest             0103512f1d8f     34 minutes ago      780MB
$USER-docker-apache.bintray.io/beam/go         latest             ce055985808a     35 minutes ago      121MB
```

<b>Although the respository names look like URLs, the container images are stored locally on your workstation.</b> After building the container images locally, you can [push](#pushing-container-images) them to an eponymous repository online.

### Overriding default Docker targets

The default SDK version is `latest` and the default Docker repository is the following Bintray location:

```
$USER-docker-apache.bintray.io/beam
```

When you [build Beam SDK container images](#building-container-images), you can override the default version and location.

To specify an older SDK version, like 2.3.0, use the `docker-tag` flag:

```
./gradlew docker -Pdocker-tag=2.3.0
```

To change the `docker` target, use the `docker-repository-root` flag:

```
./gradlew docker -Pdocker-repository-root=$LOCATION
```

## Customizing container images

You can add extra dependencies or serialization files to container images so the execution engine doesn't need them.

To customize a container image, either:
* [Write a new](#writing-new-dockerfiles) [Dockerfile](https://docs.docker.com/engine/reference/builder/) on top of the original
* [Modify](#modifying-dockerfiles) the [original Dockerfile](https://github.com/apache/beam/blob/master/sdks/python/container/Dockerfile) and reimage the container

It's often easier to write a new Dockerfile, but you can customize anything, including the base OS, by modifying the original.

### Writing new Dockerfiles on top of the original {#writing-new-dockerfiles}

<ol>
    <li>
        Pull a <a href="gcr.io/apache-beam-testing/beam/sdks/release">prebuilt SDK container image</a> for your target language and version.
    </li>
    <li>
        <a href="https://docs.docker.com/develop/develop-images/dockerfile_best-practices/">Write a new Dockerfile</a> that <a href="https://docs.docker.com/engine/reference/builder/#from">designates</a> the original as its <a href="https://docs.docker.com/glossary/?term=parent%20image">parent</a>
    </li>
    <li>
        Build the child image: <pre>docker build -f /path/to/new/Dockerfile</pre>
    </li>
</ol>

### Modifying the original Dockerfile {#modifying-dockerfiles}

1. Pull the [prebuilt SDK container image](gcr.io/apache-beam-testing/beam/sdks/release) for your target language and version.
2. Customize the [Dockerfile](https://github.com/apache/beam/blob/master/sdks/python/container/Dockerfile). If you're adding dependencies from [PyPI](https://pypi.org/), use `base_image_requirements.txt` instead of the Dockerfile.
3. [Reimage](#building-container-images) the container.

### Testing customized images

Test customized images with PortableRunner. PortableRunner targets the default Docker images, like `${USER}-docker-apache.bintray.io/beam/python${version}:latest` for the Python SDK. You can specify a different container with the `--environment_config` flag.

#### Testing on local machines

```
python -m apache_beam.examples.wordcount --input /path/to/inputfile --output /path/to/write/counts \
--runner=PortableRunner \
--job_endpoint=embed \
[--environment_config={docker_image_name}]
```

#### Testing on local Flink clusters

<ol>
    <li> Start a Flink job server on localhost:8099:
        <pre>./gradlew :runners:flink:1.5:job-server:runShadow</pre>
    </li>
    <li> Run a pipeline on the Flink job server:
<pre>python -m apache_beam.examples.wordcount --input /path/to/inputfile --output /path/to/write/counts \
--runner PortableRunner \
--job_endpoint localhost:8099 \
[--environment_config {docker_image_name}]</pre>
    </li>
</ol>

#### Testing on local Spark clusters

<ol>
    <li> Start a Spark job server on localhost:8099:
        <pre>./gradlew :runners:spark:job-server:runShadow</pre>
    </li>
    <li> Run a pipeline on the Spark job server:
<pre>python -m apache_beam.examples.wordcount --input /path/to/inputfile --output /path/to/write/counts \
--runner=PortableRunner \
--job_endpoint=localhost:8099 \
[--environment_config={docker_image_name}]</pre>
    </li>
</ol>

#### Testing on Google Cloud Dataflow

```
python -m apache_beam.examples.wordcount --input --input /path/to/inputfile --output /path/to/write/counts \
--runner=DataflowRunner \
--project={gcp_project_id} \
--temp_location={gcs_location} \
--worker_harness_container_image={docker_image_name} \
--experiment=beam_fn_api \
--sdk_location \ […]/beam/sdks/python/container/py{version}/build/target/apache-beam.tar.gz
* version={2,35,36,37}
```

## Pushing container images

Before pushing container images to your Docker registry:
* [Build](#building-container-images) the container images on your workstation
* Log in to your Bintray account with the `apache` Docker repository: ```docker login```

Run `docker push` to store the container images on Bintray. For instance, the following command pushes the Python SDK image to the `apache` Docker repository:

```
docker push $USER-docker-apache.bintray.io/beam/python:latest
```

You can run `docker pull` to download the image again:

```
docker push $USER-docker-apache.bintray.io/beam/python:latest
```

> **Note**: After pushing a container image, the remote image ID and digest match the local image ID and digest.