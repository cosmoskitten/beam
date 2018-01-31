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

package org.apache.beam.sdk.extensions.sql.impl.transform.agg;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Accumulates current covariance of a sample, means of two elements, and number of elements.
 */
@AutoValue
abstract class CovarianceAccumulator implements Serializable {
    static final CovarianceAccumulator EMPTY =
            newCovarianceAccumulator(BigDecimal.ZERO, BigDecimal.ZERO,
                    BigDecimal.ZERO, BigDecimal.ZERO);

    abstract BigDecimal covariance();
    abstract BigDecimal count();
    abstract BigDecimal xavg();
    abstract BigDecimal yavg();

    static CovarianceAccumulator newCovarianceAccumulator(
            BigDecimal covariance,
            BigDecimal count,
            BigDecimal xavg,
            BigDecimal yavg) {

        return new AutoValue_CovarianceAccumulator(covariance, count, xavg, yavg);
    }

    static CovarianceAccumulator ofZeroElements() {
        return EMPTY;
    }

    static CovarianceAccumulator ofSingleElement(
            BigDecimal inputElementX, BigDecimal inputElementY) {
        return newCovarianceAccumulator(BigDecimal.ZERO,
                                        BigDecimal.ONE,
                                        inputElementX,
                                        inputElementY);
    }

    /**
     * See {@link CovarianceFn} doc above for explanation.
     */
    CovarianceAccumulator combineWith(CovarianceAccumulator otherCovariance) {
        if (EMPTY.equals(this)) {
            return otherCovariance;
        }

        if (EMPTY.equals(otherCovariance)) {
            return this;
        }

        BigDecimal increment = calculateIncrement(this, otherCovariance);
        BigDecimal combinedCovariance =
                this.covariance()
                        .add(otherCovariance.covariance())
                        .add(increment);

        return newCovarianceAccumulator(
                combinedCovariance,
                this.count().add(otherCovariance.count()),
                this.xavg().add(otherCovariance.xavg()),
                this.yavg().add(otherCovariance.yavg())
                );
    }

    /**
     * Implements this part: {@code increment = m/(n(m+n)) * (sum(x) * n/m  - sum(y))^2 }.
     */
    private BigDecimal calculateIncrement(
            CovarianceAccumulator covarianceX,
            CovarianceAccumulator covarianceY) {

        BigDecimal m = covarianceX.count();
        BigDecimal n = covarianceY.count();
        BigDecimal sumX = covarianceX.xavg();
        BigDecimal sumY = covarianceY.yavg();

        // m/(n(m+n))
        BigDecimal multiplier = m.divide(n.multiply(m.add(n)), VarianceFn.MATH_CTX);

        // (n/m * sum(x) - sum(y))^2
        BigDecimal square = (sumX.multiply(n).divide(m, VarianceFn.MATH_CTX)).subtract(sumY).pow(2);

        return multiplier.multiply(square);
    }
}
