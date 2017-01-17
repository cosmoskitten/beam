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
package org.apache.beam.sdk.util;

import static org.hamcrest.Matchers.anyOf;
import static org.junit.Assert.fail;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test aims at keeping the public API is conformant to a hard-coded policy by
 * testing whether a package (determined by the location of the test) exposes only white listed
 * packages/classes.
 * <p>
 * Tests that derive from {@link ApiSurfaceVerificationTest} should be placed under the package to
 * be * tested and implement {@link ApiSurfaceVerificationTest#allowedClasses()}.
 * Further customization can be done by overriding {@link ApiSurfaceVerificationTest#apiSurface()}.
 * </p>
 */
// based on previous code by @kennknowles and others.
@RunWith(JUnit4.class)
public abstract class ApiSurfaceVerificationTest {

  private static final Logger LOG = LoggerFactory.getLogger(ApiSurfaceVerificationTest.class);

  private static class ClassInPackage extends TypeSafeDiagnosingMatcher<Class<?>> {

    private final String packageName;

    private ClassInPackage(final String packageName) {
      this.packageName = packageName;
    }

    @Override
    public void describeTo(final Description description) {
      description.appendText("Class in package \"");
      description.appendText(packageName);
      description.appendText("\"");
    }

    @Override
    protected boolean matchesSafely(final Class<?> clazz, final Description mismatchDescription) {
      return clazz.getName().startsWith(packageName + ".");
    }
  }

  protected static Matcher<Class<?>> inPackage(final String packageName) {
    return new ClassInPackage(packageName);
  }

  protected abstract Set<Matcher<Class<?>>> allowedClasses();

  private ApiSurface prune(final ApiSurface apiSurface, final Set<String> prunePatterns) {
    ApiSurface prunedApiSurface = apiSurface;
    for (final String prunePattern : prunePatterns) {
      prunedApiSurface = prunedApiSurface.pruningPattern(prunePattern);
    }
    return prunedApiSurface;
  }

  private void assertAbandoned(final ApiSurface checkedApiSurface,
                               final Set<Matcher<Class<?>>> allowedPackages) {

    // <helper_lambdas>

    final Function<Matcher<Class<?>>, String> toMessage =
        new Function<Matcher<Class<?>>, String>() {

          @Override
          public String apply(@Nonnull final Matcher<Class<?>> abandonedPackage) {
            final StringDescription description = new StringDescription();
            description.appendText("No ");
            abandonedPackage.describeTo(description);
            return description.toString();
          }
        };

    final Predicate<Matcher<Class<?>>> matchedByExposedClasses =
       new Predicate<Matcher<Class<?>>>() {

        @Override
        public boolean apply(@Nonnull final Matcher<Class<?>> packageMatcher) {
          return
              FluentIterable
                  .from(checkedApiSurface.getExposedClasses())
                  .anyMatch(new Predicate<Class<?>>() {

                    @Override
                    public boolean apply(@Nonnull final Class<?> aClass) {
                      return packageMatcher.matches(aClass);
                    }
                  });
        }
      };

    // </helper_lambdas>

    final ImmutableSet<Matcher<Class<?>>> matchedPackages =
        FluentIterable
        .from(allowedPackages)
        .filter(matchedByExposedClasses)
        .toSet();

    final Sets.SetView<Matcher<Class<?>>> abandonedPackages =
        Sets.difference(allowedPackages, matchedPackages);

    final ImmutableList<String> messages =
        FluentIterable
        .from(abandonedPackages)
        .transform(toMessage)
        .toSortedList(Ordering.<String>natural());

    if (!messages.isEmpty()) {
      fail("The following white-listed scopes did not have any matching exposed classes:\n\t"
               + Joiner.on("\n\t").join(messages));
    }
  }

  private void assertDisallowed(final ApiSurface checkedApiSurface,
                                final Set<Matcher<Class<?>>> allowedPackages) {

    /* <helper_lambdas> */

    final Function<Class<?>, List<Class<?>>> toExposure =
        new Function<Class<?>, List<Class<?>>>() {

          @Override
          public List<Class<?>> apply(@Nonnull final Class<?> aClass) {
            return checkedApiSurface.getAnyExposurePath(aClass);
          }
        };

    final Maps.EntryTransformer<Class<?>, List<Class<?>>, String> toMessage =
        new Maps.EntryTransformer<Class<?>, List<Class<?>>, String>() {

          @Override
          public String transformEntry(@Nonnull final Class<?> aClass,
                                       @Nonnull final List<Class<?>> exposure) {
            return aClass
                + " exposed via:\n\t\t"
                + Joiner.on("\n\t\t").join(exposure);
          }
        };

    final Predicate<Class<?>> disallowed = new Predicate<Class<?>>() {

      @Override
      public boolean apply(@Nonnull final Class<?> aClass) {
        return !classIsAllowed(aClass, allowedPackages);
      }
    };

    /* </helper_lambdas> */

    final FluentIterable<Class<?>> disallowedClasses =
        FluentIterable
            .from(checkedApiSurface.getExposedClasses())
            .filter(disallowed);

    final ImmutableMap<Class<?>, List<Class<?>>> exposures =
        Maps.toMap(disallowedClasses, toExposure);

    final ImmutableList<String> messages =
        FluentIterable
            .from(Maps.transformEntries(exposures, toMessage).values())
            .toSortedList(Ordering.<String>natural());

    if (!messages.isEmpty()) {
      fail("The following disallowed classes appear in the public API surface:\n\t"
               + Joiner.on("\n\t").join(messages));
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private boolean classIsAllowed(final Class<?> clazz,
                                 final Set<Matcher<Class<?>>>  allowedPackages) {
    // Safe cast inexpressible in Java without rawtypes
    return anyOf((Iterable) allowedPackages).matches(clazz);
  }

  private void assertApiSurface(final ApiSurface checkedApiSurface,
                                final Set<Matcher<Class<?>>> allowedPackages)
      throws Exception {

    assertDisallowed(checkedApiSurface, allowedPackages);
    assertAbandoned(checkedApiSurface, allowedPackages);
  }

  @SuppressWarnings("WeakerAccess")
  protected Set<String> prunePatterns() {
    return Sets.newHashSet("org[.]apache[.]beam[.].*Test.*",
                           "org[.]apache[.]beam[.].*IT",
                           "java[.]lang.*");
  }

  protected ApiSurface apiSurface() throws IOException {
    final String thisPackage = getClass().getPackage().getName();
    LOG.info("Verifying the surface of API package: {}", thisPackage);
    final ApiSurface apiSurface = ApiSurface.ofPackage(thisPackage);
    return prune(apiSurface, prunePatterns());
  }

  @Test
  public void testApiSurface() throws Exception {
    assertApiSurface(apiSurface(), allowedClasses());
  }
}
