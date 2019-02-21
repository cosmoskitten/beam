#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Utility to create an alias to a function;

If there is a function g, that needs to be an alias to another function f,
  g = f
would not work because g.__name__ would still be 'f'.
This is because the above creates a reference 'g' to the function 'f'.
So, they share all attributes.
Since the __name__ attribute of a function is used extensively wherever
a string needs to be formatted with the function name, this creates a
problem.
This is solved by creating a new function object with the attributes of the
function needed and assigning a desired alias to its __name__ property.
"""

from __future__ import absolute_import

import sys
import types

PY3 = sys.version_info[0] == 3


def function_alias(fn, alias):
  """Creates an alias to a function

  Args:
    fn: The function to create an alias to.
    alias: The alias of function fn.

  Returns:
    A deepcopy of the function fn with the __name__ attribute being the
    alias.
  """
  alias_fn = types.FunctionType(fn.__code__, fn.__globals__, name=alias,
                                argdefs=fn.__defaults__,
                                closure=fn.__closure__)
  alias_fn.__dict__.update(fn.__dict__)
  if PY3:
    alias_fn.__kwdefaults__ = fn.__kwdefaults__
  return alias_fn
