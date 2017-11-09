/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.annotation.audience.Audience;

/**
 * Extension to {@link cz.seznam.euphoria.core.client.dataset.windowing.Window}
 * defining time based constraints on the implementor.
 */
@Audience(Audience.Type.INTERNAL)
public interface TimedWindow {

  /**
   * Defines the timestamp/watermark until this window is considered open.
   * For time based window this is typically the stamp of the end of the window.
   *
   * @return the absolute timestamp (in milliseconds) until this window is open
   */
  long maxTimestamp();

}
