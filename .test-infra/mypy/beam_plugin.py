
from typing import Callable, Optional

from mypy.plugin import (
    Plugin, MethodContext
)
from mypy.types import (
    Type, Instance, TypeVarType
)
from mypy.expandtype import (
    expand_type
)


class BeamPlugin(Plugin):
    def get_method_hook(self, fullname: str
                        ) -> Optional[Callable[[MethodContext], Type]]:
        if fullname in {'apache_beam.pvalue.PValue.__or__',
                        'apache_beam.pvalue.PCollection.__or__'}:
            return pvalue_pipe_callback
        return None


def pvalue_pipe_callback(ctx: MethodContext) -> Type:
    """
    Callback to provide an accurate return type for
    apache_beam.pvalue.PValue.__or__.
    """
    if isinstance(ctx.type, Instance):
        assert len(ctx.arg_types) == 1, \
            'apache_beam.pvalue.PValue.__or__ should have exactly one parameter'
        assert len(ctx.arg_types[0]) == 1, \
            "apache_beam.pvalue.PValue.__or__'s parameter should not be variadic"
        transform_type = ctx.arg_types[0][0]

        args = transform_type.args
        if len(args) == 2:
            print("{}.__or__() -> {}".format(ctx.type, ctx.default_return_type))
            print("   {}".format(ctx.type.args[0]))
            print("   {}".format(transform_type))
            print("   {}".format(ctx.context.line))


            in_arg = args[0]
            if isinstance(in_arg, TypeVarType):
                expanded = expand_type(transform_type,
                                       {in_arg.id: ctx.type.args[0]})
                out_arg = expanded.args[1]
                print("   -> {}".format(expanded))
                result = ctx.default_return_type.copy_modified(
                    args=[out_arg])
                print("   -> {}".format(result))
                return result
            # for arg_type in ctx.arg_types:
            #     print("   {}".format(arg_type))
    return ctx.default_return_type


def plugin(version: str):
    # ignore version argument if the plugin works with all mypy versions.
    return BeamPlugin
