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
package org.apache.beam.sdk.io;

import static org.apache.beam.sdk.io.fs.MatchResult.Status.NOT_FOUND;

import com.google.auto.value.AutoValue;
import java.util.Collections;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Matches each filepattern in a collection of filepatterns, and produces a collection of matched
 * resources as {@link Metadata}. Resources are not deduplicated between filepatterns, i.e. if the
 * same resource matches multiple filepatterns, it will be produced multiple times.
 *
 * <p>By default, this transform matches each filepattern once and produces a bounded {@link
 * PCollection}. To continuously watch each filepattern for new files, use {@link
 * Filepatterns#watchForNewFiles(Duration, TerminationCondition)} - this will produce an unbounded
 * {@link PCollection}.
 *
 * <p>By default, filepatterns matching no files are allowed if the filepattern contains a wildcard.
 * To prohibit them (and throw an error in case one is encountered), pass {@code false} to {@link
 * Filepatterns#withEmptyMatchTreatment}.
 *
 * <p>This will produce both files and directories matching the filepattern. Use {@link Filter} or
 * other transforms to filter the output collection.
 */
public class Match {
  private static final Logger LOG = LoggerFactory.getLogger(Match.class);

  /** See {@link Match}. */
  public static Filepatterns filepatterns() {
    return new AutoValue_Match_Filepatterns.Builder()
        .setEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD)
        .build();
  }

  /** Implementation of {@link #filepatterns}. */
  @AutoValue
  public abstract static class Filepatterns
      extends PTransform<PCollection<String>, PCollection<Metadata>> {
    abstract EmptyMatchTreatment getEmptyMatchTreatment();

    @Nullable
    abstract Duration getWatchForNewFilesInterval();

    @Nullable
    abstract TerminationCondition<String, ?> getWatchForNewFilesTerminationCondition();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setEmptyMatchTreatment(EmptyMatchTreatment treatment);

      abstract Builder setWatchForNewFilesInterval(Duration watchForNewFilesInterval);

      abstract Builder setWatchForNewFilesTerminationCondition(
          TerminationCondition<String, ?> condition);

      abstract Filepatterns build();
    }

    /**
     * Sets whether or not filepatterns matching no files are allowed. When using {@link
     * #watchForNewFiles}, they are always allowed, and this parameter is ignored.
     */
    public Filepatterns withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return toBuilder().setEmptyMatchTreatment(treatment).build();
    }

    /**
     * Continuously watches for new files matching the filepattern, polling it at the given
     * interval, until the given termination condition is reached. The returned {@link PCollection}
     * is unbounded.
     *
     * <p>This works only in runners supporting {@link Experimental.Kind#SPLITTABLE_DO_FN}.
     *
     * @see TerminationCondition
     */
    @Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
    public Filepatterns watchForNewFiles(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return toBuilder()
          .setWatchForNewFilesInterval(pollInterval)
          .setWatchForNewFilesTerminationCondition(terminationCondition)
          .build();
    }

    @Override
    public PCollection<Metadata> expand(PCollection<String> input) {
      if (getWatchForNewFilesInterval() == null) {
        return input
            .apply("Expand glob", ParDo.of(new MatchFn(getEmptyMatchTreatment())))
            .setCoder(MetadataCoder.of());
      } else {
        return input
            .apply(
                "Continuously match filepatterns",
                Watch.growthOf(new MatchPollFn())
                    .withPollInterval(getWatchForNewFilesInterval())
                    .withTerminationPerInput(getWatchForNewFilesTerminationCondition())
                    .withOutputCoder(MetadataCoder.of()))
            .apply(Values.<Metadata>create());
      }
    }

    private static class MatchFn extends DoFn<String, Metadata> {
      private final EmptyMatchTreatment emptyMatchTreatment;

      public MatchFn(EmptyMatchTreatment emptyMatchTreatment) {
        this.emptyMatchTreatment = emptyMatchTreatment;
      }

      @ProcessElement
      public void process(ProcessContext c) throws Exception {
        String filepattern = c.element();
        MatchResult match = FileSystems.match(filepattern, emptyMatchTreatment);
        LOG.info("Matched {} files for pattern {}", match.metadata().size(), filepattern);
        for (Metadata metadata : match.metadata()) {
          c.output(metadata);
        }
      }
    }

    private static class MatchPollFn implements Watch.Growth.PollFn<String, Metadata> {
      @Override
      public Watch.Growth.PollResult<Metadata> apply(String input, Instant timestamp)
          throws Exception {
        MatchResult match = FileSystems.match(input);
        return Watch.Growth.PollResult.incomplete(
            Instant.now(),
            match.status() == NOT_FOUND ? Collections.<Metadata>emptyList() : match.metadata());
      }
    }
  }
}
