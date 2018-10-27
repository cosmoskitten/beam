---
layout: section
section_menu: section-menu/contribute.html
title: "Python tips"
permalink: /contribute/python-tips/
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

# Python tips

### Developing with the Python SDK

Gradle can build and test python, and is used by the Jenkins jobs, so needs to
be maintained.

You can directly use the Python toolchain instead of having Gradle orchestrate
it, which may be faster for you, but it is your preference.
If you do want to use Python tools directly, we recommend setting up a virtual
environment before testing your code.

If you update any of the [cythonized](http://cython.org) files in Python SDK,
you must install the `cython` package before running following command to
properly test your code.

The following commands should be run in the `sdks/python` directory.
This installs Python from source and includes the test and gcp dependencies.

On macOS/Linux:

    $ virtualenv env
    $ . ./env/bin/activate
    (env) $ pip install -e .[gcp,test]

On Windows:

    > c:\Python27\python.exe -m virtualenv
    > env\Scripts\activate
    (env) > pip install -e .[gcp,test]

This command runs all Python tests. The nose dependency is installed by [test] in pip install.

    (env) $ python setup.py nosetests

You can use following command to run a single test method.

    (env) $ python setup.py nosetests --tests <module>:<test class>.<test method>

    For example:
    (env) $ python setup.py nosetests --tests apache_beam.io.textio_test:TextSourceTest.test_progress

You can deactivate the virtualenv when done.

    (env) $ deactivate
    $

To check just for Python lint errors, run the following command.

    $ ../../gradlew lint

Or use `tox` commands to run the lint tasks:

    $ tox -e py27-lint    # For python 2.7
    $ tox -e py3-lint     # For python 3
    $ tox -e py27-lint3   # For python 2-3 compatibility

#### Remote testing

This step is only required for testing SDK code changes remotely (not using
directrunner). In order to do this you must build the Beam tarball. From the
root of the git repository, run:

```
$ cd sdks/python/
$ python setup.py sdist
```

Pass the `--sdk_location` flag to use the newly built version. For example:

```
$ python setup.py sdist > /dev/null && \
    python -m apache_beam.examples.wordcount ... \
        --sdk_location dist/apache-beam-2.5.0.dev0.tar.gz
```
