---
layout: section
section_menu: section-menu/contribute.html
title: "Website tips"
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

# Website tips

The website is written with [Jekyll](https://jekyllrb.com/) markdown.

For small changes, you can use the edit button at the top right of each page.

For larger changes, you can run:

    $ ./gradlew :beam-website:serveWebsite

Which will start a docker container and serve the website to [http://localhost:4000](http://localhost:4000).
Ater one hour, the docker container will exit and you will need to re-start it.


You can also check links with:

    $ ./gradlew :beam-website:testWebsite


