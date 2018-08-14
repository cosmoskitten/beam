package org.apache.beam.sdk.options;

import java.util.SortedSet;

public class PipelineOptionUnexpectedPropertyException extends IllegalArgumentException {
  private static final long serialVersionUID = 3265630128856068164L;

  private String message;

  public PipelineOptionUnexpectedPropertyException(String var1) {
    super(var1);
  }

  public void createUnexpectedPropertyExceptionMessage(String propertyName) {
    message =
        String.format(
            "Pipeline option '%s' is not supported in this context. It can be "
                + "cleared by 'RESET %s'.",
            propertyName, propertyName);
  }

  public void createUnexpectedPropertyExceptionMessage(String propertyName, String bestMatch) {
    createUnexpectedPropertyExceptionMessage(propertyName);
    message += String.format(" Did you mean 'SET %s'?", bestMatch);
  }

  public void createUnexpectedPropertyExceptionMessage(
      String propertyName, SortedSet<String> bestMatches) {
    createUnexpectedPropertyExceptionMessage(propertyName);
    message += String.format(" Did you mean SET one of %s?", bestMatches);
  }

  public String getUnexpectedPropertyExceptionMessage() {
    return message;
  }
}
