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
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.seznam.euphoria.core.client.functional;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import java.util.stream.Stream;

/**
 * Reduce function reducing iterable of elements into single element (of
 * possibly different type).
 */
@Audience(Audience.Type.CLIENT)
@FunctionalInterface
public interface ReduceFunction<IN, OUT> extends UnaryFunction<Stream<IN>, OUT> {

}
