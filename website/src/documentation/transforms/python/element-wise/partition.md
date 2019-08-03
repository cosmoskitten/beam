---
layout: section
title: "Partition"
permalink: /documentation/transforms/python/elementwise/partition/
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

# Partition

<script type="text/javascript">
localStorage.setItem('language', 'language-py')
</script>

<table>
  <td>
    <a class="button" target="_blank"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.Partition">
      <img src="https://beam.apache.org/images/logos/sdks/python.png"
          width="20px" height="20px" alt="Pydoc" />
      Pydoc
    </a>
  </td>
</table>
<br>
Separates elements in a collection into multiple output
collections. The partitioning function contains the logic that determines how
to separate the elements of the input collection into each resulting
partition output collection.

The number of partitions must be determined at graph construction time.
You cannot determine the number of partitions in mid-pipeline

See more information in the [Beam Programming Guide]({{ site.baseurl }}/documentation/programming-guide/#partition).

## Examples

In the following examples, we create a pipeline with a `PCollection` of produce their icon, name, and duration.
Then, we apply `Partition` in multiple ways to split the `PCollection` into multiple `PCollections`.

`Partition` accepts a function that receives the number of partitions,
and returns the index of the desired partition for the element.
The number of partitions passed must be a positive integer,
and it must return an integer in the range `0` to `num_partitions-1`.

### Example 1: Partition with a function

In the following example, we have a known list of durations.
We partition the `PCollection` into one `PCollection` for every duration type.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition.py tag:partition_function %}```

Output `PCollection`s:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition_test.py tag:partitions %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Example 2: Partition with a lambda function

We can also use lambda functions to simplify **Example 1**.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition.py tag:partition_lambda %}```

Output `PCollection`s:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition_test.py tag:partitions %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Example 3: Partition with multiple arguments

You can pass functions with multiple arguments to `Partition`.
They are passed as additional positional arguments or keyword arguments to the function.

In this example, `split_dataset` takes `plant`, `num_partitions`, and `ratio` as arguments.
`num_partitions` is used by `Partitions` as a positional argument,
while any other argument will be passed to `split_dataset`.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition.py tag:partition_multiple_arguments %}```

Output `PCollection`s:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition_test.py tag:train_test %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

## Related transforms

* [Filter]({{ site.baseurl }}/documentation/transforms/python/elementwise/filter) is useful if the function is just
  deciding whether to output an element or not.
* [ParDo]({{ site.baseurl }}/documentation/transforms/python/elementwise/pardo) is the most general element-wise mapping
  operation, and includes other abilities such as multiple output collections and side-inputs.
* [CoGroupByKey]({{ site.baseurl }}/documentation/transforms/python/aggregation/cogroupbykey)
performs a per-key equijoin.

<table>
  <td>
    <a class="button" target="_blank"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.Partition">
      <img src="https://beam.apache.org/images/logos/sdks/python.png"
          width="20px" height="20px" alt="Pydoc" />
      Pydoc
    </a>
  </td>
</table>
<br>
