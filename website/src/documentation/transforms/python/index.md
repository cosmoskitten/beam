---
layout: section
title: "Python transform catalog overview"
permalink: /documentation/transforms/python/overview
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

# Python transform catalog overview

## Element-wise
[Filter]({{ site.baseurl }}/documentation/transforms/python/elementwise/filter)
: Given a predicate, filter out all elements that don't satisfy the predicate.

[FlatMap]({{ site.baseurl }}/documentation/transforms/python/elementwise/flatmap)
: Applies a function that returns a collection to every element in the input and
  outputs all resulting elements.

[Keys]({{ site.baseurl }}/documentation/transforms/python/elementwise/keys)
: Extracts the key from each element in a collection of key-value pairs.

[KvSwap]({{ site.baseurl }}/documentation/transforms/python/elementwise/kvswap)
: Swaps the key and value of each element in a collection of key-value pairs.

[Map]({{ site.baseurl }}/documentation/transforms/python/elementwise/map)
: Applies a function to every element in the input and outputs the result.

[ParDo]({{ site.baseurl }}/documentation/transforms/python/elementwise/pardo)
: The most-general mechanism for applying a user-defined `DoFn` to every element
  in the input collection.

[Partition]({{ site.baseurl }}/documentation/transforms/python/elementwise/partition)
: Routes each input element to a specific output collection based on some partition
  function.

[Timestamp]({{ site.baseurl }}/documentation/transforms/python/elementwise/timestamp)
: Applies a function to determine a timestamp to each element in the output collection,
  and updates the implicit timestamp associated with each input. Note that it is only
  safe to adjust timestamps forwards.

[Values]({{ site.baseurl }}/documentation/transforms/python/elementwise/values)
: Extracts the value from each element in a collection of key-value pairs.

## Aggregation 
[GroupByKey]({{ site.baseurl }}/documentation/transforms/python/aggregation/groupbykey)
: Takes a keyed collection of elements and produces a collection where each element 
  consists of a key and all values associated with that key.

[Mean]({{ site.baseurl }}/documentation/transforms/python/aggregation/mean)
: Computes the average within each aggregation.

[RemoveDuplicates]({{ site.baseurl }}/documentation/transforms/python/aggregation/removeduplicates)
: Produces a collection containing distinct elements from the input collection.

## Other
[Flatten]({{ site.baseurl }}/documentation/transforms/python/other/flatten)
: Given multiple input collections, produces a single output collection containing
  all elements from all of the input collections.

[Reshuffle]({{ site.baseurl }}/documentation/transforms/python/other/reshuffle)
: Given an input collection, redistributes the elements between workers. This is
  most useful for adjusting paralellism or preventing coupled failures.

[Window]({{ site.baseurl }}/documentation/transforms/python/other/window)
: Logically divides up or groups the elements of a collection into finite
  windows according to a function.