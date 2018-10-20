package org.apache.beam.sdk.extensions.euphoria.core.translate;

import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.ShuffleOperator;

public class OutputValues<InputT, KeyT, OutputT> extends Operator<OutputT> {

  private final ShuffleOperator<InputT, KeyT, OutputT>shuffleOperator;

  OutputValues(ShuffleOperator<InputT, KeyT, OutputT> shuffleOperator) {
    shuffleOperator.getK
    super(shuffleOperator.getName());
    this.shuffleOperator = shuffleOperator;
  }
}
