/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.hcatalog;

import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO.Read;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hive.hcatalog.common.HCatUtil;

/** Utility classes to enable meta store conf/client creation. */
public class HCatalogUtils {

  static IMetaStoreClient createMetaStoreClient(Configuration conf)
      throws IOException, MetaException {
    final HiveConf hiveConf = HCatUtil.getHiveConf(conf);
    return HCatUtil.getHiveMetastoreClient(hiveConf);
  }

  static HiveConf createHiveConf(Read readRequest) throws IOException {
    Configuration conf = new Configuration();
    for (Map.Entry<String, String> entry : readRequest.getConfigProperties().entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    return HCatUtil.getHiveConf(conf);
  }
}
