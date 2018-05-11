/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.type;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

/** TODO: complete javadoc. */
@Audience(Audience.Type.EXECUTOR)
public class TypeAwareUnaryFunction<InT, OutT>
    extends AbstractTypeAware<UnaryFunction<InT, OutT>, OutT> implements UnaryFunction<InT, OutT> {

  private TypeAwareUnaryFunction(UnaryFunction<InT, OutT> function, TypeHint<OutT> resultType) {
    super(function, resultType);
  }

  public static <InT, OutT> TypeAwareUnaryFunction<InT, OutT> of(
      UnaryFunction<InT, OutT> function, TypeHint<OutT> typeHint) {
    return new TypeAwareUnaryFunction<>(function, typeHint);
  }

  @Override
  public OutT apply(InT what) {
    return getDelegate().apply(what);
  }
}
