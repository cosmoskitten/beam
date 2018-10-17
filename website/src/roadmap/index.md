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
Project Management Committee (PMC), so we do not (and cannot) have a roadmap in
the sense of a plan with a specific timeline.

Instead, we highlight our vision for the future and major initiatives receiving
a attention and contribution that users can look forward to. It is broken down
by major component of Beam.

## Portability Framework

The primary Beam vision: Any SDK on any runner. This is a cross-cutting effort
across Java, Python, and Go, and every Beam runner.

 - [Read more]({{ site.baseurl }}/contribute/portability/)

## Apache Spark 2.0 Runner

 - Feature branch: [runners-spark2](https://github.com/apache/beam/tree/runners-spark2)
 - Contact: [Jean-Baptiste Onofré](mailto:jbonofre@apache.org)

## Go SDK

The Go SDK is the newest SDK, and is the first SDK built entirely on the
portability framework.

 - JIRA: [sdk-go](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20sdk-go) / [BEAM-2083](https://issues.apache.org/jira/browse/BEAM-2083) |
 - Contact: [Henning Rohde](mailto:herohde@google.com)

## Python SDK

The Python SDK has multiple initiatives ongoing.

### Python 3 Support

Work is in progress to add Python 3 support to Beam.  Current goal is to make Beam codebase compatible both with Python 2.7 and Python 3.4.

## Java SDK

The Java SDK is the oldest SDK, and the core is quite mature. It predates the
portability framework, so adding portability support is ongoing.
Much development is focused on all the pieces that make it usable and
production-ready, like new DSLs, IO connectors, and adding portability support.

### Next Java LTS version support (Java 11 / 18.9)

Work to support the next LTS release of Java is in progress. For more details about the scope and info on the various tasks please see the JIRA ticket.

- JIRA: [BEAM-2530](https://issues.apache.org/jira/browse/BEAM-2530)
- Contact: [Ismaël Mejía](mailto:iemejia@gmail.com)

### IO Performance Testing

We are working on performance Tests for IOs; expect connectors to steadily improve their performance.

### Euphoria Java 8 DSL

Easy to use Java 8 DSL for the Beam Java SDK

(link to Euphoria roadmap, or summarize)

## Beam SQL

Beam SQL is built on the Java SDK but also has some preliminary steps towards a
"pure SQL" way to author Beam pipelines. A lot of work is going into making
Beam SQL a rock-solid implementation with no missing pieces and great
performance.

 - JIRA: [dsl-sql](https://issues.apache.org/jira/issues/?filter=12343977)
 - Contact: [Kenneth Knowles](mailto:kenn@apache.org)

## Performance improvements

We have a major focus on benchmarking and improving performance on all runners. Expect steady improvements here.
