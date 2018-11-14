---
layout: section
title: "Euphoria API Roadmap"
permalink: /roadmap/euphoria/
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

# Euphoria API

Easy to use Java 8 DSL for the Beam Java SDK. Provides a high-level abstraction of Beam transformations, which is both easy to read and write. Can be used as a complement to existing Beam pipelines (convertible back and forth). You can have a glimpse of the API at [WordCount example]({{ site.baseurl
}}/documentation/sdks/java/euphoria/#wordcount-example).

- JIRA: [dsl-euphoria](https://issues.apache.org/jira/browse/BEAM-4366?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20dsl-euphoria) / [BEAM-3900](https://issues.apache.org/jira/browse/BEAM-3900)
- Contact: [David Moravek](mailto:dmvk@apache.org)

## "Salted" join implementation

Implementation of a join, that can handle large scale join of highly skewed data sets. This implementation breaks
the large keys into multiple splits, using key distribution approximated by count min sketch data structure. 

## Pipeline sampling

In order to pick the right translation for the operator without user interference, we can leverage knowledge from
previous pipeline runs. We want to provide a convenient and portable way to gather this knowledge.

## Reusing keyed PCollections

If we have a `PCollection` that is already keyed, we can reuse this and simplify operator builder.

Before:

```java
final PCollection<KV<String, Integer>> counts = ...;

SumByKey.named("SummarizeCounts")
  .of(counts)
  .keyBy(KV:getKey)
  .valueBy(KV::getValue)
  .output();
```

After:

```java
final PCollection<KV<String, Integer>> counts = ...;

SumByKey.named("SummarizeCounts")
  .ofKeyed(counts)
  .valueBy(KV::getValue)
  .output();
```

## Side Outputs

We plan on providing convenient API for multiple outputs. Eg.:

```java
final PCollection<Integer> numbers = ...;
final SideOutput<Log> logs = SideOutput.of(Log.class);

final PCollection<Integers> multiplied = FlatMap.named("MultiplyNumbers")
  .of(numbers)
  .using((num, coll) -> {
    coll.output(num * 1000);
    coll.output(logs, "Multiplied " + num + ".");
  })
  .output();

final PCollection<Integers> divided = FlatMap.named("DivideNumbers")
  .of(numbers)
  .using((num, coll) -> {
    coll.output(num / 1000);
    coll.output(logs, "Divded" + num + ".");
  })
  .withSideOutputs(logs)
  .output();

MapElements.named("DoSomethingWithLogs")
  .of(logs.getPCollection())
  .using(...)
  .withSideOutputs(logs)
  .output()
```
