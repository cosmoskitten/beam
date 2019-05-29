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
package org.apache.beam.sdk.extensions.sql;

import com.google.zetasql.Analyzer;
import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.ZetaSQLBuiltinFunctionOptions;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import org.junit.Test;

/** UnitTest for Explain Plan. */
public class ZetaSqlTest {
  @Test
  public void testZetaSql() {
    System.setProperty(
        "org.apache.beam.repackaged.beam_sdks_java_extensions_sql.packagePrefix",
        "org.apache.beam.repackaged.beam_sdks_java_extensions_sql.");
    SimpleCatalog simpleCatalog = new SimpleCatalog("beam");
    simpleCatalog.addZetaSQLFunctions(new ZetaSQLBuiltinFunctionOptions());
    AnalyzerOptions analyzerOptions = new AnalyzerOptions();
    Analyzer analyzer = new Analyzer(analyzerOptions, simpleCatalog);
    ResolvedStatement resolvedStatement = analyzer.analyzeStatement("SELECT 1 AS INT64");
  }
}
