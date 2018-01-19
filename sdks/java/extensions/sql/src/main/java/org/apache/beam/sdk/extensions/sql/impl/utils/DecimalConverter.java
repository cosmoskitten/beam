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

package org.apache.beam.sdk.extensions.sql.impl.utils;

import java.math.BigDecimal;

/**
 * Converters from {@link BigDecimal} ot other numeric types.
 */
public enum DecimalConverter {
  INTEGER {
    @Override
    public Integer convert(BigDecimal decimalValue) {
      return decimalValue.intValue();
    }
  },
  SHORT {
    @Override
    public Short convert(BigDecimal decimalValue) {
      return decimalValue.shortValue();
    }
  },
  BYTE {
    @Override
    public Byte convert(BigDecimal decimalValue) {
      return decimalValue.byteValue();
    }
  },
  LONG {
    @Override
    public Long convert(BigDecimal decimalValue) {
      return decimalValue.longValue();
    }
  },
  FLOAT {
    @Override
    public Float convert(BigDecimal decimalValue) {
      return decimalValue.floatValue();
    }
  },
  DOUBLE {
    @Override
    public Double convert(BigDecimal decimalValue) {
      return decimalValue.doubleValue();
    }
  },
  BIGDECIMAL {
    @Override
    public BigDecimal convert(BigDecimal decimalValue) {
      return decimalValue;
    }
  };

  public abstract <T> T convert(BigDecimal value);
}
