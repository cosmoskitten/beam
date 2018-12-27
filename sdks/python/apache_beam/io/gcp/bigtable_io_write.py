import logging
import apache_beam as beam
from google.cloud import bigtable
from apache_beam.io import iobase
from apache_beam.metrics import Metrics
from apache_beam.io.iobase import SourceBundle
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display import HasDisplayData
from google.cloud.bigtable.batcher import MutationsBatcher
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker


class WriteToBigtable(beam.DoFn):
	""" Creates the connector can call and add_row to the batcher using each
	row in beam pipe line

	:type beam_options: class:`~bigtable_configuration.BigtableConfiguration`
	:param beam_options: Class `~bigtable_configuration.BigtableConfiguration`.

	:type flush_count: int
	:param flush_count: (Optional) Max number of rows to flush. If it
	reaches the max number of rows it calls finish_batch() to mutate the
	current row batch. Default is FLUSH_COUNT (1000 rows).

	:type max_mutations: int
	:param max_mutations: (Optional)  Max number of row mutations to flush.
	If it reaches the max number of row mutations it calls finish_batch() to
	mutate the current row batch. Default is MAX_MUTATIONS (100000 mutations).

	:type max_row_bytes: int
	:param max_row_bytes: (Optional) Max number of row mutations size to
	flush. If it reaches the max number of row mutations size it calls
	finish_batch() to mutate the current row batch. Default is MAX_ROW_BYTES
	(5 MB).

	:type app_profile_id: str
	:param app_profile_id: (Optional) The unique name of the AppProfile.
	"""

	def __init__(self, beam_options):
		super(WriteToBigtable, self).__init__(beam_options)
		self.beam_options = beam_options
		self.client = None
		self.instance = None
		self.table = None
		self.batcher = None
		self._app_profile_id = self.beam_options.app_profile_id
		self.flush_count = self.beam_options.flush_count
		self.max_row_bytes = self.beam_options.max_row_bytes
		self.written = Metrics.counter(self.__class__, 'Written Row')

	def start_bundle(self):
		if self.beam_options.credentials is None:
			self.client = bigtable.Client(project=self.beam_options.project_id,
										  admin=True)
		else:
			self.client = bigtable.Client(
				project=self.beam_options.project_id,
				credentials=self.beam_options.credentials,
				admin=True)
		self.instance = self.client.instance(self.beam_options.instance_id)
		self.table = self.instance.table(self.beam_options.table_id,
										 self._app_profile_id)

		self.batcher = MutationsBatcher(
			self.table, flush_count=self.flush_count,
			max_row_bytes=self.max_row_bytes)
		self.written = Metrics.counter(self.__class__, 'Written Row')

	def process(self, row):
		self.written.inc()
		self.batcher.mutate(row)

	def finish_bundle(self):
		return self.batcher.flush()

	def display_data(self):
		return {
			'projectId': DisplayDataItem(self.beam_options.project_id, label='Bigtable Project Id'),
			'instanceId': DisplayDataItem(self.beam_options.instance_id, label='Bigtable Instance Id'),
			'tableId': DisplayDataItem(self.beam_options.table_id, label='Bigtable Table Id'),
			'bigtableOptions': DisplayDataItem(str(self.beam_options), label='Bigtable Options', key='bigtableOptions'),
		}


class BigtableConfiguration(object):
	""" Bigtable configuration variables.

	:type project_id: :class:`str` or :func:`unicode <unicode>`
	:param project_id: (Optional) The ID of the project which owns the
						instances, tables and data. If not provided, will
						attempt to determine from the environment.

	:type instance_id: str
	:param instance_id: The ID of the instance.

	:type table_id: str
	:param table_id: The ID of the table.
	"""

	def __init__(self, project_id, instance_id, table_id):
		self.project_id = project_id
		self.instance_id = instance_id
		self.table_id = table_id
		self.credentials = None

class BigtableWriteConfiguration(BigtableConfiguration):
	"""
	:type flush_count: int
	:param flush_count: (Optional) Max number of rows to flush. If it
	reaches the max number of rows it calls finish_batch() to mutate the
	current row batch. Default is FLUSH_COUNT (1000 rows).
	:type max_mutations: int
	:param max_mutations: (Optional)  Max number of row mutations to flush.
	If it reaches the max number of row mutations it calls finish_batch() to
	mutate the current row batch. Default is MAX_MUTATIONS (100000 mutations).
	:type max_row_bytes: int
	:param max_row_bytes: (Optional) Max number of row mutations size to
	flush. If it reaches the max number of row mutations size it calls
	finish_batch() to mutate the current row batch. Default is MAX_ROW_BYTES
	(5 MB).
	:type app_profile_id: str
	:param app_profile_id: (Optional) The unique name of the AppProfile.
	"""

	def __init__(self, project_id, instance_id, table_id, flush_count=None, max_row_bytes=None,
				 app_profile_id=None):
		super(BigtableWriteConfiguration, self).__init__(project_id, instance_id, table_id)
		self.flush_count = flush_count
		self.max_row_bytes = max_row_bytes
		self.app_profile_id = app_profile_id

	def __str__(self):
		import json
		return json.dumps({
			'project_id': self.project_id,
			'instance_id': self.instance_id,
			'table_id': self.table_id,
		})
