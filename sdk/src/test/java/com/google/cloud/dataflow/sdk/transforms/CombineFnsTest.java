/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.transforms;

import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Combine.BinaryCombineFn;
import com.google.cloud.dataflow.sdk.transforms.CombineFns.CoCombineResult;
import com.google.cloud.dataflow.sdk.transforms.CombineWithContext.KeyedCombineFnWithContext;
import com.google.cloud.dataflow.sdk.transforms.Max.MaxIntegerFn;
import com.google.cloud.dataflow.sdk.transforms.Min.MinIntegerFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.ImmutableList;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link CombineFns}.
 */
@RunWith(JUnit4.class)
public class CombineFnsTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testDuplicatedTags() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("it is already present in the composition");

    TupleTag<Integer> tag = new TupleTag<Integer>();
    CombineFns.compose()
      .with(new GetIntegerFunction(), new MaxIntegerFn(), tag)
      .with(new GetIntegerFunction(), new MinIntegerFn(), tag);
  }

  @Test
  public void testDuplicatedTagsKeyed() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("it is already present in the composition");

    TupleTag<Integer> tag = new TupleTag<Integer>();
    CombineFns.composeKeyed()
      .with(new GetIntegerFunction(), new MaxIntegerFn(), tag)
      .with(new GetIntegerFunction(), new MinIntegerFn(), tag);
  }

  @Test
  public void testDuplicatedTagsWithContext() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("it is already present in the composition");

    TupleTag<UserString> tag = new TupleTag<UserString>();
    CombineFns.compose()
      .with(
          new GetUserStringFunction(),
          new ConcatStringWithContext(null /* view */).forKey("G", StringUtf8Coder.of()),
          tag)
      .with(
          new GetUserStringFunction(),
          new ConcatStringWithContext(null /* view */).forKey("G", StringUtf8Coder.of()),
          tag);
  }

  @Test
  public void testDuplicatedTagsWithContextKeyed() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("it is already present in the composition");

    TupleTag<UserString> tag = new TupleTag<UserString>();
    CombineFns.composeKeyed()
      .with(
          new GetUserStringFunction(),
          new ConcatStringWithContext(null /* view */),
          tag)
      .with(
          new GetUserStringFunction(),
          new ConcatStringWithContext(null /* view */),
          tag);
  }

  @Test
  @Category(RunnableOnService.class)
  public void testComposedCombine() {
    Pipeline p = TestPipeline.create();
    p.getCoderRegistry().registerCoder(UserString.class, UserStringCoder.of());

    PCollection<KV<String, KV<Integer, UserString>>> perKeyInput = p.apply(
        Create.timestamped(
            Arrays.asList(
                KV.of("a", KV.of(1, UserString.of("1"))),
                KV.of("a", KV.of(1, UserString.of("1"))),
                KV.of("a", KV.of(4, UserString.of("4"))),
                KV.of("b", KV.of(1, UserString.of("1"))),
                KV.of("b", KV.of(13, UserString.of("13")))),
            Arrays.asList(0L, 4L, 7L, 10L, 16L))
        .withCoder(KvCoder.of(
            StringUtf8Coder.of(),
            KvCoder.of(BigEndianIntegerCoder.of(), UserStringCoder.of()))));

    TupleTag<Integer> maxIntTag = new TupleTag<Integer>();
    TupleTag<UserString> concatStringTag = new TupleTag<UserString>();
    PCollection<KV<String, KV<Integer, String>>> combineGlobally = perKeyInput
        .apply(Values.<KV<Integer, UserString>>create())
        .apply(Combine.globally(CombineFns.compose()
            .with(
                new GetIntegerFunction(),
                new MaxIntegerFn(),
                maxIntTag)
            .with(
                new GetUserStringFunction(),
                new ConcatString(),
                concatStringTag)))
        .apply(WithKeys.<String, CoCombineResult>of("global"))
        .apply(
            "ExtractGloballyResult", ParDo.of(new ExtractResultDoFn(maxIntTag, concatStringTag)));

    PCollection<KV<String, KV<Integer, String>>> combinePerKey = perKeyInput
        .apply(Combine.perKey(CombineFns.composeKeyed()
            .with(
                new GetIntegerFunction(),
                new MaxIntegerFn().<String>asKeyedFn(),
                maxIntTag)
            .with(
                new GetUserStringFunction(),
                new ConcatString().<String>asKeyedFn(),
                concatStringTag)))
        .apply("ExtractPerKeyResult", ParDo.of(new ExtractResultDoFn(maxIntTag, concatStringTag)));
    DataflowAssert.that(combineGlobally).containsInAnyOrder(
        KV.of("global", KV.of(13, "111134")));
    DataflowAssert.that(combinePerKey).containsInAnyOrder(
        KV.of("a", KV.of(4, "114")),
        KV.of("b", KV.of(13, "113")));
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testComposedCombineWithContext() {
    Pipeline p = TestPipeline.create();
    p.getCoderRegistry().registerCoder(UserString.class, UserStringCoder.of());

    PCollectionView<String> view = p
        .apply(Create.of("I"))
        .apply(View.<String>asSingleton());

    PCollection<KV<String, KV<Integer, UserString>>> perKeyInput = p.apply(
        Create.timestamped(
            Arrays.asList(
                KV.of("a", KV.of(1, UserString.of("1"))),
                KV.of("a", KV.of(1, UserString.of("1"))),
                KV.of("a", KV.of(4, UserString.of("4"))),
                KV.of("b", KV.of(1, UserString.of("1"))),
                KV.of("b", KV.of(13, UserString.of("13")))),
            Arrays.asList(0L, 4L, 7L, 10L, 16L))
        .withCoder(KvCoder.of(
            StringUtf8Coder.of(),
            KvCoder.of(BigEndianIntegerCoder.of(), UserStringCoder.of()))));

    TupleTag<Integer> maxIntTag = new TupleTag<Integer>();
    TupleTag<UserString> concatStringTag = new TupleTag<UserString>();
    PCollection<KV<String, KV<Integer, String>>> combineGlobally = perKeyInput
        .apply(Values.<KV<Integer, UserString>>create())
        .apply(Combine.globally(CombineFns.compose()
            .with(
                new GetIntegerFunction(),
                new MaxIntegerFn(),
                maxIntTag)
            .with(
                new GetUserStringFunction(),
                new ConcatStringWithContext(view).forKey("G", StringUtf8Coder.of()),
                concatStringTag))
            .withoutDefaults()
            .withSideInputs(ImmutableList.of(view)))
        .apply(WithKeys.<String, CoCombineResult>of("global"))
        .apply(
            "ExtractGloballyResult", ParDo.of(new ExtractResultDoFn(maxIntTag, concatStringTag)));

    PCollection<KV<String, KV<Integer, String>>> combinePerKey = perKeyInput
        .apply(Combine.perKey(CombineFns.composeKeyed()
            .with(
                new GetIntegerFunction(),
                new MaxIntegerFn().<String>asKeyedFn(),
                maxIntTag)
            .with(
                new GetUserStringFunction(),
                new ConcatStringWithContext(view),
                concatStringTag))
            .withSideInputs(ImmutableList.of(view)))
        .apply("ExtractPerKeyResult", ParDo.of(new ExtractResultDoFn(maxIntTag, concatStringTag)));
    DataflowAssert.that(combineGlobally).containsInAnyOrder(
        KV.of("global", KV.of(13, "111134GI")));
    DataflowAssert.that(combinePerKey).containsInAnyOrder(
        KV.of("a", KV.of(4, "114Ia")),
        KV.of("b", KV.of(13, "113Ib")));
    p.run();
  }

  private static class UserString implements Serializable {
    private String strValue;

    static UserString of(String strValue) {
      UserString ret = new UserString();
      ret.strValue = strValue;
      return ret;
    }
  }

  private static class UserStringCoder extends StandardCoder<UserString> {
    public static UserStringCoder of() {
      return INSTANCE;
    }

    private static final UserStringCoder INSTANCE = new UserStringCoder();

    @Override
    public void encode(UserString value, OutputStream outStream, Context context)
        throws CoderException, IOException {
      StringUtf8Coder.of().encode(value.strValue, outStream, context);
    }

    @Override
    public UserString decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      return UserString.of(StringUtf8Coder.of().decode(inStream, context));
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }

  private static class GetIntegerFunction
      extends SimpleFunction<KV<Integer, UserString>, Integer> {
    @Override
    public Integer apply(KV<Integer, UserString> input) {
      return input.getKey();
    }
  }

  private static class GetUserStringFunction
      extends SimpleFunction<KV<Integer, UserString>, UserString> {
    @Override
    public UserString apply(KV<Integer, UserString> input) {
      return input.getValue();
    }
  }

  private static class ConcatString extends BinaryCombineFn<UserString> {
    @Override
    public UserString apply(UserString left, UserString right) {
      String retStr = left.strValue + right.strValue;
      char[] chars = retStr.toCharArray();
      Arrays.sort(chars);
      return UserString.of(new String(chars));
    }
  }

  private static class ConcatStringWithContext
      extends KeyedCombineFnWithContext<String, UserString, UserString, UserString> {
    private final PCollectionView<String> view;

    private ConcatStringWithContext(PCollectionView<String> view) {
      this.view = view;
    }

    @Override
    public UserString createAccumulator(String key, CombineWithContext.Context c) {
      return UserString.of(key + c.sideInput(view));
    }

    @Override
    public UserString addInput(
        String key, UserString accumulator, UserString input, CombineWithContext.Context c) {
      assertThat(accumulator.strValue, Matchers.startsWith(key + c.sideInput(view)));
      accumulator.strValue += input.strValue;
      return accumulator;
    }

    @Override
    public UserString mergeAccumulators(
        String key, Iterable<UserString> accumulators, CombineWithContext.Context c) {
      String keyPrefix = key + c.sideInput(view);
      String all = keyPrefix;
      for (UserString accumulator : accumulators) {
        assertThat(accumulator.strValue, Matchers.startsWith(keyPrefix));
        all += accumulator.strValue.substring(keyPrefix.length());
        accumulator.strValue = "cleared in mergeAccumulators";
      }
      return UserString.of(all);
    }

    @Override
    public UserString extractOutput(
        String key, UserString accumulator, CombineWithContext.Context c) {
      assertThat(accumulator.strValue, Matchers.startsWith(key + c.sideInput(view)));
      char[] chars = accumulator.strValue.toCharArray();
      Arrays.sort(chars);
      return UserString.of(new String(chars));
    }
  }

  private static class ExtractResultDoFn
      extends DoFn<KV<String, CoCombineResult>, KV<String, KV<Integer, String>>>{

    private final TupleTag<Integer> maxIntTag;
    private final TupleTag<UserString> concatStringTag;

    ExtractResultDoFn(TupleTag<Integer> maxIntTag, TupleTag<UserString> concatStringTag) {
      this.maxIntTag = maxIntTag;
      this.concatStringTag = concatStringTag;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      KV<Integer, String> value = KV.of(
          c.element().getValue().get(maxIntTag),
          c.element().getValue().get(concatStringTag).strValue);
      c.output(KV.of(c.element().getKey(), value));
    }
  }
}
