---
layout: section
section_menu: section-menu/contribute.html
title: "Git workflow"
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

To propose code changes, you should be familiar with the Git / Github workflow, including:
   - [forking](https://help.github.com/articles/fork-a-repo/) the
     [github.com/apache/beam](https://github.com/apache/beam) repo
   - [cloning](https://help.github.com/articles/cloning-a-repository/) your fork
   - [syncing](https://help.github.com/articles/syncing-a-fork/) your master branch with the upstream apache/beam
     repository
   - [branching](https://help.github.com/articles/creating-and-deleting-branches-within-your-repository/)
   - [squashing](https://help.github.com/articles/about-git-rebase/) commits
   - [proposing changes](https://help.github.com/articles/proposing-changes-to-your-work-with-pull-requests/) in a pull
     request

For example, to create a pull request:

1. [Fork](https://help.github.com/articles/fork-a-repo/) the [github.com/apache/beam](https://github.com/apache/beam) repo
1. [Clone](https://help.github.com/articles/cloning-a-repository/) your fork, for example:

       $ git clone git@github.com:${GITHUB_USERNAME}/beam
       $ cd beam

1. Add an upstream remote for apache/beam to allow syncing changes into your fork

       $ git remote add upstream https://github.com/apache/beam

1. Create a local branch for your changes

       $ git checkout -b someBranch

1. Make your code change
1. Add unit tests for your change
1. Ensure tests pass locally
1. Commit your change with the name of the jira issue

       $ git add <new files>
       $ git commit -am "[BEAM-xxxx] Description of change"

1. Push your change to your forked repo

       $ git push --set-upstream origin YOUR_BRANCH_NAME

1. Browse to the URL of your forked repository and propose a pull request.

1. Find a committer to review, and @mention them in the review comments

