package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.operator.test.ng.junit.Processing;
import cz.seznam.euphoria.operator.test.ng.junit.Processing.Type;
import cz.seznam.euphoria.operator.test.ng.tests.AllOperatorsSuite;

@Processing(Type.BOUNDED) // spark supports only bounded processing
public class NgSparkOperatorTest
    extends AllOperatorsSuite
    implements NgSparkExecutorProvider {}
