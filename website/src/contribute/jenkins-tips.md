---
layout: section
section_menu: section-menu/contribute.html
title: "Jenkins tips"
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

# Jenkins tips

The Jenkins jobs are defined in ./test-infra/jenkins. These jobs are written using the
[Job DSL](https://jenkinsci.github.io/job-dsl-plugin/), using [Apache Groovy](http://groovy-lang.org/).

Job definitions should be a simple as possible, and ideally identify a single gradle target to execute.

# Testing changes

To test changes locally, without breaking the project's job definitions,
you can use [Local Dockerized Jenkins](https://github.com/apache/beam/tree/master/.test-infra/dockerized-jenkins).

It is possible to test a PR that includes Jenkins changes, but it is potentially destructive because the job
definitions are a shared resource. To update the job definitions on a PR, use the "Run Seed Job" trigger phrase.
This will cause the job definitions to be read and parsed, and they will be active until the next
scheduled execution on [beam_SeedJob](https://builds.apache.org/job/beam_SeedJob/) or
[beam_SeedJob_Standalone](https://builds.apache.org/job/beam_SeedJob_Standalone/).

# Triggering jobs

Beam committers can trigger a job with the jenkins UI. Non-committers can trigger a job if there is a trigger
phrase.

# Job suffixes

Each post-commit and pre-commit job file defines several jobs with different suffixes. For pre-commits, there _Commit,
_Phrase, and _Cron suffixes. The _Commit job happens with every push to a pull request. The _Phrase happens when the
trigger phrase is entered as a comment in the pre-commit. The _Cron pre-commit happens post-commit on the master
branch as a signal whether the pre-commit would pass without any changes.

# Job labels
Most beam Jenkins jobs specify the label [beam](https://builds.apache.org/label/beam/), which uses beam executors 1-15.

The performance test jobs specify the label [beam-perf](https://builds.apache.org/label/beam-perf/), which uses beam
executors 16.

The website publishing job specifies the label [git-websites](https://builds.apache.org/label/git-websites/), which
allows publishing generated documentation to the asf-site branch.