---
layout: section
section_menu: section-menu/contribute.html
title: "Beam Contribution FAQ"
permalink: /contribute/FAQ/
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

## Apache Beam Contribution FAQ

Frequently asked questions about contributing to Apache Beam.

<!-- TODO(BEAM-5737): Move this to confluence wiki -->

### Who do I ask if I am stuck?

Either email [dev@beam.apache.org]({{ site.baseurl }}/community/contact-us/) or chat
on [Slack]({{ site.baseurl }}/community/contact-us/).

### Why can't I assign a jira issue to myself?

Only ASF Jira contributors can be assigned issues. To get the permission to do so, email
the [dev@ mailing list]({{ site.baseurl }}/community/contact-us)
to ask to be a contributor in the Beam issue tracker. For example:

```
Hi, my name is <your name>. I am interested in contributing <what> to the Apache Beam SDK. I'd like to be added
as a Jira contributor so that I can assign issues to myself. My ASF Jira Username is <username>.
```

### OutOfMemoryException during the Gradle build

You might get an OutOfMemoryException during the Gradle build. If you have more memory
available, you can try to increase the memory allocation of the Gradle JVM. Otherwise,
disabling parallel test execution reduces memory consumption. In the root of the Beam
source, edit the `gradle.properties` file and add/modify the following lines:

    org.gradle.parallel=false
    org.gradle.jvmargs=-Xmx2g -XX:MaxPermSize=512m

### Finding reviewers

Currently this is a manual process. Tracking bug for automating this:
[BEAM-4790](https://issues.apache.org/jira/browse/BEAM-4790).

Reviewers for [apache/beam](https://github.com/apache/beam) are listed in
Prow-style OWNERS files. A description of these files can be found
[here](https://go.k8s.io/owners).

For each file to be reviewed, look for an OWNERS file in its directory. Pick a
single reviewer from that file. If the directory doesn't contain an OWNERS file,
go up a directory. Keep going until you find one. Try to limit the number of
reviewers to 2 per PR if possible, to minimize reviewer load. Comment on your PR
tagging the reviewer as follows:

    R: @reviewer

### Opt-in to reviewing pull requests

Find the deepest sub-directory that contains the files you want to be a reviewer
for and add your Github username under `reviewers` in the OWNERS file (create a
new OWNERS file if necessary).

The Beam project currently only uses the `reviewers` key in OWNERS and no other
features, as reviewer selection is still a manual process.

<!-- TODO(BEAM-4790): If Prow write access gets approved
(https://issues.apache.org/jira/browse/INFRA-16869), document that if you are
not a committer you can still be listed as a reviewer. Just ask to get added as
a read-only collaborator to apache/beam by opening an INFRA ticket. -->


