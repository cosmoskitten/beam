package org.apache.beam.codestyle;

import java.util.List;

/**
 * Class to be formatted using beam-codestyle.xml to test it is configured correctly.
 */
public class CodeStyleTestClass {

  public static MyClass withSideInputs(List<?>... sideInputs) {
    return new MyClass().myMethod(sideInputs);
  }

  /**
   * Fake Class.
   */
  public static class MyClass {

    private static final String MY_CONSTANT = "";

    MyClass() {
    }

    public MyClass myMethod(List<?>... sideInputs) {
      return null;
    }
  }
}
