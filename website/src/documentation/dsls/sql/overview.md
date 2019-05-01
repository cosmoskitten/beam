---
layout: section
title: "Beam SQL: Overview"
section_menu: section-menu/sdks.html
permalink: /documentation/dsls/sql/overview/
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
# Beam SQL: Overview

Beam SQL allows a Beam user (currently only available in Beam Java) to query
bounded and unbounded `PCollections` with SQL statements. Your SQL query
is translated to a `PTransform`, an encapsulated segment of a Beam pipeline.
You can freely mix SQL `PTransforms` and other `PTransforms` in your pipeline.

[Apache Calcite](http://calcite.apache.org) provides the basic dialect underlying
Beam SQL. Calcite is a widespread SQL dialect used in
big data processing with some streaming enhancements. We have added additional extensions to
make it easy to leverage Beam's unified batch/streaming model and support
for complex data types.

There are two additional concepts need to know to use SQL in your pipeline:
 - [SqlTransform](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/extensions/sql/SqlTransform.html): 
   the interface for creating `PTransforms` from SQL queries.
 - [Row](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/values/Row.html):
   the type of elements that Beam SQL operates on. A `PCollection<Row>` plays the role of a table.

## SQL pipeline walkthrough
The [SQL pipeline walkthrough]({{ site.baseurl
}}/documentation/dsls/sql/walkthrough) works through how you use
Beam SQL with example code.

## Beam SQL shell
The [Beam SQL shell]({{ site.baseurl }}/documentation/dsls/sql/shell/) page shows you how to work with the interactive shell. You can use the interactive Beam SQL shell to write pipelines as SQL queries without needing the Java SDK. 

## Apache Calcite dialect
The [Calcite dialect pages]({{ site.baseurl }}/documentation/dsls/sql/calcite/overview/) show you which operators and functions supported by Beam SQL and describes the built-in Beam SQL extensions for Calcite.

## Beam SQL extensions
The [Beam SQL extensions]({{ site.baseurl }}/documentation/dsls/sql/extensions/create-external-table/) pages serve as a reference for additional built-in extensions. 
