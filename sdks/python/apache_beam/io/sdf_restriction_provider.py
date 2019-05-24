from apache_beam.io.restriction_trackers import SDFBoundedSourceRestrictionTracker
from apache_beam.transforms.core import RestrictionProvider

__all__ = ['SDFBoundedSourceRestrictionProvider']



class SDFBoundedSourceRestrictionProvider(RestrictionProvider):
  def __init__(self, source, desired_chunk_size=None):
    self._source = source
    self._desired_chunk_size = desired_chunk_size
    # The size cannot be calculated directly by end_pos - start_pos since the
    # position may not be numeric(e.g., key space position).
    self._restriction_size_map = {}

  def initial_restriction(self, element):
    # Get initial range_tracker from source
    range_tracker = self._source.get_range_tracker(None, None)
    return (range_tracker.start_position(), range_tracker.stop_position())

  def create_tracker(self, restriction):
    return SDFBoundedSourceRestrictionTracker(
        self._source.get_range_tracker(restriction[0],
                                      restriction[1]))

  def split(self, element, restriction):
    # Invoke source.split to get initial splitting results.
    source_bundles = self._source.split(self._desired_chunk_size)
    for source_bundle in source_bundles:
      self._restriction_size_map[(source_bundle.start_position,
                                  source_bundle.stop_position)] = source_bundle.weight
      yield (source_bundle.start_position, source_bundle.stop_position)

  def restriction_size(self, element, restriction):
    return self._restriction_size_map[(restriction[0], restriction[1])]

