---
layout: section
title: "RemoveDuplicates"
permalink: /documentation/transforms/python/aggregation/removeduplicates/
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

# RemoveDuplicates
[Pydoc](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html?highlight=removeduplicates#apache_beam.transforms.util.RemoveDuplicates)

Produces a collection containing distinct elements from the input collection.
Does not preserve the order of the input collection.

You can provide a function that computes representative values, such as an event ID,
from an element.
