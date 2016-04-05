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

package com.google.cloud.dataflow.sdk.options;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.Sets;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Set;

/**
 * Unit tests for {@link PipelineOptionsReflector}.
 */
@RunWith(JUnit4.class)
public class PipelineOptionsReflectorTest {
  @Test
  public void testPropertyExtraction() throws NoSuchMethodException {
    Set<PipelineOptionsReflector.Property> properties =
        PipelineOptionsReflector.collectVisibleProperties(SimpleOptions.class);

    assertThat(properties, Matchers.hasItems(PipelineOptionsReflector.Property.of(
        SimpleOptions.class, "foo", SimpleOptions.class.getDeclaredMethod("getFoo"))));
  }

  interface SimpleOptions extends PipelineOptions {
    String getFoo();
    void setFoo(String value);
  }

  @Test
  public void testFiltersNonGetterMethods() {
    Set<PipelineOptionsReflector.Property> properties =
        PipelineOptionsReflector.collectVisibleProperties(OnlyTwoValidGetters.class);

    assertThat(properties, not(hasItem(hasName(isOneOf("misspelled", "hasParameter", "prefix")))));
  }

  interface OnlyTwoValidGetters extends PipelineOptions {
    String getFoo();
    void setFoo(String value);

    boolean isBar();
    void setBar(boolean value);

    String gtMisspelled();
    void setMisspelled(String value);

    String getHasParameter(String value);
    void setHasParameter(String value);

    String noPrefix();
    void setNoPrefix(String value);
  }

  @Test
  public void testBaseClassOptions() {
    Set<PipelineOptionsReflector.Property> props =
        PipelineOptionsReflector.collectVisibleProperties(ExtendsSimpleOptions.class);

    assertThat(props, Matchers.hasItems(
        allOf(hasName("foo"), hasClass(SimpleOptions.class)),
        allOf(hasName("foo"), hasClass(ExtendsSimpleOptions.class)),
        allOf(hasName("bar"), hasClass(ExtendsSimpleOptions.class))));
  }

  interface ExtendsSimpleOptions extends SimpleOptions {
    @Override String getFoo();
    @Override void setFoo(String value);

    String getBar();
    void setBar(String value);
  }

  @Test
  public void testExcludesNonPipelineOptionsMethods() {
    Set<PipelineOptionsReflector.Property> properties =
        PipelineOptionsReflector.collectVisibleProperties(ExtendsNonPipelineOptions.class);

    assertThat(properties, not(hasItem(hasName("foo"))));
  }

  interface NoExtendsClause {
    String getFoo();
    void setFoo(String value);
  }

  interface ExtendsNonPipelineOptions extends NoExtendsClause, PipelineOptions {}

  @Test
  public void testExcludesHiddenInterfaces() {
    Set<PipelineOptionsReflector.Property> properties =
        PipelineOptionsReflector.collectVisibleProperties(Hidden.class);

    assertThat(properties, not(hasItem(hasName("foo"))));
  }

  @com.google.cloud.dataflow.sdk.options.Hidden
  interface Hidden extends PipelineOptions {
    String getFoo();
    void setFoo(String value);
  }


  @Test
  public void testMultipleInputInterfaces() {
    Set<PipelineOptionsReflector.Property> props =
        PipelineOptionsReflector.collectVisibleProperties(
            Sets.<Class<? extends PipelineOptions>>newHashSet(
                BaseOptions.class, ExtendOptions1.class, ExtendOptions2.class));

    assertThat(props, Matchers.hasItems(
        allOf(hasName("baseOption"), hasClass(BaseOptions.class)),
        allOf(hasName("extendOption1"), hasClass(ExtendOptions1.class)),
        allOf(hasName("extendOption2"), hasClass(ExtendOptions2.class))));
  }

  interface BaseOptions extends PipelineOptions {
    String getBaseOption();
    void setBaseOption(String value);
  }

  interface ExtendOptions1 extends BaseOptions {
    String getExtendOption1();
    void setExtendOption1(String value);
  }

  interface ExtendOptions2 extends BaseOptions {
    String getExtendOption2();
    void setExtendOption2(String value);
  }

  private static Matcher<PipelineOptionsReflector.Property> hasName(String name) {
    return hasName(is(name));
  }

  private static Matcher<PipelineOptionsReflector.Property> hasName(Matcher<String> matcher) {
    return new FeatureMatcher<PipelineOptionsReflector.Property, String>(matcher, "name", "name") {
      @Override
      protected String featureValueOf(PipelineOptionsReflector.Property actual) {
        return actual.name();
      }
    };
  }

  private static Matcher<PipelineOptionsReflector.Property> hasClass(Class<?> clazz) {
    return new FeatureMatcher<PipelineOptionsReflector.Property, Class<?>>(
        Matchers.<Class<?>>is(clazz), "defining class", "class") {
      @Override
      protected Class<?> featureValueOf(PipelineOptionsReflector.Property actual) {
        return actual.definingInterface();
      }
    };
  }

  private static Matcher<PipelineOptionsReflector.Property> hasGetter(String methodName) {
    return new FeatureMatcher<PipelineOptionsReflector.Property, String>(
        is(methodName), "getter method", "name") {
      @Override
      protected String featureValueOf(PipelineOptionsReflector.Property actual) {
        return actual.getterMethod().getName();
      }
    };
  }
}
