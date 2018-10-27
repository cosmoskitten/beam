---
layout: section
section_menu: section-menu/contribute.html
title: "Gradle tips"
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

# Gradle tips

The [gradle](https://docs.gradle.org) build files are written using [Apache Groovy](http://groovy-lang.org/).

Gradle targets can be specified with either the project name or directory, for example:

    $ ./gradlew :beam-website:serveWebsite

is the same as

    $ ./gradlew -p website serveWebsite

The mapping of directory to project name is in
[settings.gradle](https://github.com/apache/beam/blob/master/settings.gradle)

To troubleshoot issues:
- the `--info` flag adds verbose logging
- the `--stacktrace` flag adds a stacktrace on failures
- the `--scan` flag creates a web scan page

Most re-usable steps are in [BeamModulePlugin.groovy](
https://github.com/apache/beam/blob/master/buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy).

A common style in the build files for methods with several arguments is to use
the [named argument constructor](
http://docs.groovy-lang.org/docs/groovy-2.5.0-beta-1/html/documentation/#_named_argument_constructor)
which allows passing a map for names and values, for example:

```
class BuildTaskConfiguration {
  String name
  boolean useTestConfig = false
  String baseUrl = ''
  String dockerWorkDir = ''
}

def createBuildTask = {
  BuildTaskConfiguration config = it as BuildTaskConfiguration
...
}

createBuildTask(name:'Local', dockerWorkDir: dockerWorkDir)
```