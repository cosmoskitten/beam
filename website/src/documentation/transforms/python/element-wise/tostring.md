---
layout: section
title: "ToString"
permalink: /documentation/transforms/python/elementwise/tostring/
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

# ToString

<script type="text/javascript">
localStorage.setItem('language', 'language-py')
</script>

Transforms every element in an input collection to a string.

## Example

Any non-string element can be converted to a string using standard Python functions and methods.
Many I/O transforms, such as `TextIO`, expect their input elements to be strings.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/to_string.py tag:to_string %}```

Output `PCollection` as strings:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/to_string_test.py tag:plants %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/to_string.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

## Related transforms

* [Map]({{ site.baseurl }}/documentation/transforms/python/elementwise/map) applies a simple 1-to-1 mapping function over each element in the collection
