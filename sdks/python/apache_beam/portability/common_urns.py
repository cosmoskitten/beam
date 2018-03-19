from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import standard_window_fns_pb2

class PropertiesFromEnumValue(object):
  def __init__(self, value_descriptor):
    self.urn = (
        value_descriptor.GetOptions().Extensions[beam_runner_api_pb2.beam_urn])

class PropertiesFromEnumType(object):
  def __init__(self, enum_type):
    for v in enum_type.DESCRIPTOR.values:
      setattr(self, v.name, PropertiesFromEnumValue(v))

primitives = PropertiesFromEnumType(
    beam_runner_api_pb2.StandardPTransforms.Primitives)
deprecated_primitives = PropertiesFromEnumType(
    beam_runner_api_pb2.StandardPTransforms.DeprecatedPrimitives)
composites = PropertiesFromEnumType(
    beam_runner_api_pb2.StandardPTransforms.Composites)
combine_components = PropertiesFromEnumType(
    beam_runner_api_pb2.StandardPTransforms.CombineComponents)

side_inputs = PropertiesFromEnumType(
    beam_runner_api_pb2.StandardSideInputTypes.Enum)

coders = PropertiesFromEnumType(beam_runner_api_pb2.StandardCoders.Enum)

coders.VARINT.urn = coders.VARINT.urn
coders.ITERABLE.urn = coders.ITERABLE.urn
coders.INTERVAL_WINDOW.urn = coders.INTERVAL_WINDOW.urn
coders.LENGTH_PREFIX.urn = coders.LENGTH_PREFIX.urn
coders.GLOBAL_WINDOW.urn = coders.GLOBAL_WINDOW.urn
coders.WINDOWED_VALUE.urn = coders.WINDOWED_VALUE.urn

def PropertiesFromPayloadType(payload_type):
  return PropertiesFromEnumType(payload_type.Enum).PROPERTIES

global_windows = PropertiesFromPayloadType(
    standard_window_fns_pb2.GlobalWindowsPayload)
fixed_windows = PropertiesFromPayloadType(
    standard_window_fns_pb2.FixedWindowsPayload)
sliding_windows = PropertiesFromPayloadType(
    standard_window_fns_pb2.SlidingWindowsPayload)
session_windows = PropertiesFromPayloadType(
    standard_window_fns_pb2.SessionsPayload)
