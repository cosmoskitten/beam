---
layout: section
title: "Beam Roadmap"
permalink: /roadmap/
section_menu: section-menu/roadmap.html
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

# Apache Beam Roadmap

Apache Beam is not governed or steered by any one commercial entity, but by its
Project Management Committee (PMC), so we do not have a roadmap in the sense of
a plan with a specific timeline.

Instead, we share our vision for the future and major initiatives receiving
a attention and contribution that users can look forward to. It is broken down
by major component of Beam.

Below are some highlights for the project as a whole; some individual components
have enough upcoming that they have their own roadmap as well.

## Portability Framework

The primary Beam vision: Any SDK on any runner. This is a cross-cutting effort
across Java, Python, and Go, and every Beam runner.

## Portable Flink Runner / Python on Flink

Support for portability is being added to the Flink runner, driven by users
trying out the Python SDK on the Flink runner.

## Go SDK

The Go SDK is the newest SDK, and is the first SDK built entirely on the
portability framework. (something something about where you can run it and
where people are trying to run it)

## Python 3 support

... link into Python SDK roadmap ...

## Java 11 support

... link into Java SDK roadmap ...

## SQL

Beam SQL is built on the Java SDK but also has some preliminary steps towards a
"pure SQL" way to author Beam pipelines. (say something)

## Performance

The community is actively building infrastructure for measuring and monitoring
performance of runners and IO connectors written atop the SDKs.

