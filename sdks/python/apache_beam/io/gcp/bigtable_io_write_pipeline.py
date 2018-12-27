from apache_beam.options.pipeline_options import PipelineOptions

class ReadBigtableOptions(PipelineOptions):
	""" Create the Pipeline Options to set ReadBigtable/WriteBigtable.
	You can create and use this class in the Template, with a certainly steps.
	"""
	@classmethod
	def _add_argparse_args(cls, parser):
		super(ReadBigtableOptions, cls)._add_argparse_args(parser)
		parser.add_argument('--instance', required=True )
		parser.add_argument('--table', required=True )
