<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

For the test of [HCatalogIO](../../../../../../../../main/java/org/apache/beam/sdk/io/hcatalog/HCatalogIO.java) 
with independent infrastructure was prepared `dataproc` service with HCatalog.

To setup similar infrastructure on `dataproc` run following command:

```
gcloud dataproc clusters create hcatalog \
     --initialization-actions gs://dataproc-initialization-actions/hive-hcatalog/hive-hcatalog.sh
```

Next open Hadoop/Hive/HCatalog ports to make those services visible for the test.
Test were run in same network as infrastructure.

The test should pass for infrastructure set up locally by installing: Hadoop, Hive (with HCatalog included).  

Details about how to run the test can be found in: 
[HCatalogIOIT](HCatalogIOIT.java). 
