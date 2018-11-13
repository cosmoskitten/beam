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

// contains Big query related properties for Nexmark runs
class NexmarkBuilder {

  enum Runner {
    DATAFLOW("DataflowRunner", ":beam-runners-google-cloud-dataflow-java"),
    SPARK("SparkRunner", ":beam-runners-spark"),
    FLINK("FlinkRunner", ":beam-runners-flink_2.11"),
    DIRECT("DirectRunner", ":beam-runners-direct-java")

    private final String option
    private final String dependency

    Runner(String option, String dependency) {
      this.option = option
      this.dependency = dependency
    }
  }

  enum Mode {
    STREAMING,
    BATCH

    boolean isStreaming() {
      return this == STREAMING
    }
  }

  enum TriggeringContext {
    PR,
    POST_COMMIT
  }

  enum QueryLanguage {
    JAVA,
    SQL
  }

  static void job(context, Runner runner, List<String> runnerSpecificOptions, TriggeringContext triggeringContext) {
    context.steps {
      suite(context, "NEXMARK IN BATCH MODE USING ${runner} RUNNER", runner, runnerSpecificOptions,  Mode.BATCH, triggeringContext)
      suite(context, "NEXMARK IN STREAMING MODE USING ${runner} RUNNER", runner, runnerSpecificOptions, Mode.STREAMING, triggeringContext)
      suite(context, "NEXMARK IN SQL BATCH MODE USING ${runner} RUNNER", runner, runnerSpecificOptions, Mode.BATCH, triggeringContext, QueryLanguage.SQL)
      suite(context, "NEXMARK IN SQL STREAMING MODE USING ${runner} RUNNER", runner, runnerSpecificOptions, Mode.STREAMING, triggeringContext, QueryLanguage.SQL)
    }
  }

  static void suite(context,
             String title,
             Runner runner,
             List<String> runnerSpecificOptions,
             Mode mode,
             TriggeringContext triggeringContext,
             QueryLanguage queryLanguage = QueryLanguage.JAVA) {
    context.shell("echo *** RUN ${title} ***")
    context.gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':beam-sdks-java-nexmark:run')
      commonJobProperties.setGradleSwitches(context)
      switches('-Pnexmark.runner=' + runner.dependency +
              ' -Pnexmark.args="' + pipelineOptions(runner, mode, determineBigQueryDataset(triggeringContext), runnerSpecificOptions, queryLanguage))
    }
  }

  private static String determineBigQueryDataset(TriggeringContext triggeringContext) {
    triggeringContext == TriggeringContext.PR ? "nexmark_PRs" : "nexmark"
  }

  private
  static String pipelineOptions(Runner runner, Mode mode, String bqDataset,
                                List<String> runnerSpecificOptions, QueryLanguage queryLanguage) {
    def options = ['--bigQueryTable=nexmark',
                   "--bigQueryDataset=${bqDataset}",
                   '--project=apache-beam-testing',
                   '--resourceNameMode=QUERY_RUNNER_AND_MODE',
                   '--exportSummaryToBigQuery=true',
                   '--tempLocation=gs://temp-storage-for-perf-tests/nexmark',
                   "--runner=${runner.option}",
                   "--streaming=${mode.isStreaming()}",
                   '--manageResources=false',
                   '--monitorJobs=true',
                   ]

    options = options + runnerSpecificOptions

    if (queryLanguage != QueryLanguage.JAVA) {
      options.add("--queryLanguage={$queryLanguage}")
    }

    return options.join(' ')
  }
}
