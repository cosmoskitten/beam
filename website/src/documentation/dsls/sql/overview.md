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

# Beam SQL overview

Beam SQL allows a Beam user (currently only available in Beam Java) to query
bounded and unbounded `PCollections` with SQL statements. Your SQL query
is translated to a `PTransform`, an encapsulated segment of a Beam pipeline.
You can freely mix SQL `PTransforms` and other `PTransforms` in your pipeline.

Beam SQL supports the following dialects:

- [Apache Calcite](http://calcite.apache.org)
- [ZetaSQL](#zetasql)

[Apache Calcite](http://calcite.apache.org) is the default. It's a widespread SQL dialect used in
big data processing. ZetaSQL is more compatible with BigQuery, so especially useful in pipelines that [write to or read from BigQuery tables]({{ site.baseurl
}}/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.html). To change dialects, call the [`setPlannerName()`]({{ site.baseurl
}}/releases/javadoc/current/org/apache/beam/sdk/extensions/sql/impl/BeamSqlPipelineOptions.html#setPlannerName-java.lang.String-) method in [`BeamSqlPipelineOptions`]({{ site.baseurl
}}/releases/javadoc/current/org/apache/beam/sdk/extensions/sql/impl/BeamSqlPipelineOptions.html).

There are two additional concepts you need to know to use SQL in your pipeline:

 - [SqlTransform](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/extensions/sql/SqlTransform.html): the interface for creating `PTransforms` from SQL queries.
 - [Row](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/values/Row.html):
   the type of elements that Beam SQL operates on. A `PCollection<Row>` plays the role of a table.

## Walkthrough
The [SQL pipeline walkthrough]({{ site.baseurl
}}/documentation/dsls/sql/walkthrough) works through how to use Beam SQL with example code.

## Shell
The Beam SQL shell allows you to write pipelines as SQL queries without using the Java SDK. 
The [Shell page]({{ site.baseurl
}}/documentation/dsls/sql/shell) describes how to work with the interactive Beam SQL shell. 

## Apache Calcite dialect 
The [Calcite overview]({{ site.baseurl
}}/documentation/dsls/sql/calcite/overview) summarizes Apache Calcite operators,
functions, syntax, and data types supported by Beam SQL.

## ZetaSQL dialect
For more information on the ZetaSQL features in Beam SQL, see the ZetaSQL dialect reference.

To switch to the ZetaSQL dialect, run [`setPlannerName("org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner")`]({{ site.baseurl
}}/releases/javadoc/current/org/apache/beam/sdk/extensions/sql/impl/BeamSqlPipelineOptions.html#setPlannerName-java.lang.String-).

## Beam SQL extensions
Beam SQL has additional [extensions]({{ site.baseurl
}}/documentation/dsls/sql/extensions/create-external-table) to
make it easy to leverage Beam's unified batch/streaming model and support
for complex data types.