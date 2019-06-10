import argparse
import subprocess as sp


def parse_example():
  """Parse the command line arguments and return it as a string function call.
  Examples:
    python path/to/snippets.py function_name
    python path/to/snippets.py function_name arg1
    python path/to/snippets.py function_name arg1 arg2 ... argN
  """
  parser = argparse.ArgumentParser()
  parser.add_argument('example', help='Name of the example to run.')
  parser.add_argument('args', nargs=argparse.REMAINDER,
                      help='Arguments for example.')
  args = parser.parse_args()

  # Return the example as a function call.
  example_args = ', '.join([repr(arg) for arg in args.args])
  return '{}({})'.format(args.example, example_args)


def run_shell_commands(commands, **kwargs):
  """Runs a list of Notebook-like shell commands.

  Lines starting with `#` are ignored as comments.
  Lines starting with `!` are run as commands.
  Variables surrounded by `{variable}` are substituted with **kwargs.
  """
  for cmd in commands:
    cmd = cmd.strip().lstrip('!')
    if not cmd or cmd.startswith('#'):
      continue
    cmd = cmd.format(**kwargs)
    sp.call(cmd.split())
