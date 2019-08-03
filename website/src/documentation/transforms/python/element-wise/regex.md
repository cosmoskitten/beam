---
layout: section
title: "Regex"
permalink: /documentation/transforms/python/elementwise/regex/
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

# Regex

<script type="text/javascript">
localStorage.setItem('language', 'language-py')
</script>

Filters input string elements based on a regex. May also transform them based on the matching groups.

## Examples

In the following examples, we create a pipeline with a `PCollection` of text strings.
Then, we use the `re` module to search, replace, and split through the text elements using
[regular expressions](https://docs.python.org/3/library/re.html).

You can use tools to help you create and test your regular expressions such as
[regex101](https://regex101.com/),
make sure to specify the Python flavor at the left side bar.

### Example 1: Regex match

[`re.match`](https://docs.python.org/3/library/re.html#re.match)
will try to match the regular expression from the beginning of the string.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py tag:regex_match %}```

Output `PCollection` after regex:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex_test.py tag:plant_matches %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Example 2: Regex search

[`re.search`](https://docs.python.org/3/library/re.html#re.search)
will try to search for the first occurrence the regular expression in the string.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py tag:regex_search %}```

Output `PCollection` after regex:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex_test.py tag:plant_matches %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Example 3: Regex find all

[`re.finditer`](https://docs.python.org/3/library/re.html#re.finditer)
will try to search for all the occurrence the regular expression in the string.
This returns an iterator of match objects.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py tag:regex_find_all %}```

Output `PCollection` after regex:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex_test.py tag:words %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Example 4: Regex replace

[`re.sub`](https://docs.python.org/3/library/re.html#re.sub)
will substitute occurrences the regular expression in the string.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py tag:regex_replace %}```

Output `PCollection` after regex:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex_test.py tag:plants_csv %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Example 5: Regex split

[`re.split`](https://docs.python.org/3/library/re.html#re.split)
will split the string on every ocurrence of the regular expression.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py tag:regex_split %}```

Output `PCollection` after regex:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex_test.py tag:plants_columns %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

## Related transforms

* [FlatMap]({{ site.baseurl }}/documentation/transforms/python/elementwise/flatmap) behaves the same as `Map`, but for
  each input it may produce zero or more outputs.
* [Map]({{ site.baseurl }}/documentation/transforms/python/elementwise/map) applies a simple 1-to-1 mapping function over each element in the collection
