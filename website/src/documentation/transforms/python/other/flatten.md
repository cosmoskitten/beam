---
layout: section
title: "Flatten"
permalink: /documentation/transforms/python/other/flatten/
section_menu: section-menu/documentation.html
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

# Flatten
[Pydoc](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html?highlight=flatten#apache_beam.transforms.core.Flatten)

Flatten is a Beam transform for `PCollection` objects that store the same
data type. `Flatten` merges multiple `PCollection` objects into a single logical
`PCollection`.

## See also
* [FlatMap]({{ site.baseurl }}/documentation/transforms/python/elementwise/flatmap) applies a simple 1-to-many mapping
  function over each element in the collection. This transform might produce zero
  or more outputs.