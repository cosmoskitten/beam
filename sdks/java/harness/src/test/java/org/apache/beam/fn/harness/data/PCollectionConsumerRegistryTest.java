package org.apache.beam.fn.harness.data;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;


/** Tests for {@link PCollectionConsumerRegistryTest}. */
@RunWith(JUnit4.class)
public class PCollectionConsumerRegistryTest {

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void multipleConsumersSamePCollection() throws Exception {
    final String pCollectionA = "pCollectionA";

    PCollectionConsumerRegistry consumers = new PCollectionConsumerRegistry();
    FnDataReceiver<WindowedValue<String>> consumerA1 = mock(FnDataReceiver.class);
    FnDataReceiver<WindowedValue<String>> consumerA2 = mock(FnDataReceiver.class);

    consumers.register(pCollectionA, consumerA1);
    consumers.register(pCollectionA, consumerA2);

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)(FnDataReceiver)
            consumers.getSingleOrMultiplexingConsumer(pCollectionA);

    WindowedValue<String> element = WindowedValue.valueInGlobalWindow("elem");
    int numElements = 20;
    for (int i = 0; i < numElements; i++) {
      wrapperConsumer.accept(element);
    }

    // Check that the underlying consumers are each invoked per element
    // and the multiplexing one is invoked only once.
    verify(consumerA1, times(numElements)).accept(element);
    verify(consumerA2, times(numElements)).accept(element);

    assertThat(consumers.keySet(), contains(pCollectionA));
    assertThat(consumers.getUnderlyingConsumers(pCollectionA), contains(consumerA1, consumerA2));
  }
}
