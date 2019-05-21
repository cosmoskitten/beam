from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.transforms.core import RestrictionProvider
from apache_beam.transforms import ptransform
from apache_beam.transforms import core

__all__ = ['SDFBoundedSourceRestrictionProvider']

class SDFBoundedSourceRestrictionProvider(RestrictionProvider):
  def __init__(self, source, desired_chunk_size=None):
    self._source = source
    self._desired_chunk_size = desired_chunk_size

  def initial_restriction(self, element):
    # Get initial range_tracker from source
    range_tracker = self._source.get_range_tracker(None, None)
    return (range_tracker.start_position(), range_tracker.stop_position())

  def create_tracker(self, restriction):
    # Make an assumption here: OffsetRestrictionTracker works for all BoundedSource.
    # Current limitation: OffsetRestrictionTracker can only track integer
    # Possible TODO: have a another RestrictionTracker?
    return OffsetRestrictionTracker(restriction[0], restriction[1])

  def split(self, element, restriction):
    # Invoke source.split to get initial splitting results.
    source_bundles = self._source.split(self._desired_chunk_size)
    for source_bundle in source_bundles:
      yield (source_bundle.start_position, source_bundle.stop_position)

  def restriction_size(self, element, restriction):
    # More precisely, this function returns the weight, rather the actual size.
    return restriction[1] - restriction[0]



class SDFBoundedSourceWrapper(ptransform.PTransform):
  def __init__(self, source):
    from apache_beam.options.pipeline_options import DebugOptions
    debug_options = self.pipeline._options.view_as(DebugOptions)
    if (debug_options.experiments is None or
        'beam_fn_api' not in debug_options.experiments):
      raise RuntimeError('Using SDF expects fnapi enabled')
    super(SDFBoundedSourceWrapper, self).__init__()
    self.source = source

  def _create_sdf_bounded_source_dofn(self):
    total_size = self.source.estimate_size()
    source = self.source
    if total_size:
      # 1MB = 1 shard, 1GB = 32 shards, 1TB = 1000 shards, 1PB = 32k shards
      chunk_size = max(1 << 20, 1000 * int(math.sqrt(total_size)))
    else:
      chunk_size = 64 << 20  # 64mb

    class SDFBoundedSourceDoFn(core.DoFn):
      def __init__(self, read_source):
        self.source = read_source
      def process(
          self,
          element,
          restriction_tracker=core.DoFn.RestrictionParam(
              SDFBoundedSourceRestrictionProvider(source, chunk_size))):
        start_pos, end_pos = restriction_tracker.current_restriction()
        range_tracker = self.source.get_range_tracker(start_pos, end_pos)
        return self.source.read(range_tracker)

    return SDFBoundedSourceDoFn(self.source)


  def expand(self, pbegin):
    return (pbegin
            | core.Impulse()
            | core.ParDo(self._create_sdf_bounded_source_dofn()))
