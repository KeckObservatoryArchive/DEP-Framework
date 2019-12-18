"""
This is a template pipeline.

"""

from keckdrpframework.pipelines.base_pipeline import BasePipeline
from ..primitives.dep_obtain import *
from ..primitives.dep_locate import *
from ..primitives.dep_add import *
from ..primitives.dep_dqa import *

class dep_pipeline(BasePipeline):
    """
    The template pipeline.
    """

    event_table = {
        # this is a standard primitive defined in the framework
        "next_file": ("simple_fits_reader", "file_ready", "template_primitive"),
        # this is a primitive defined in the template primitive files
        "template_primitive": ("Template", "running_template_primitive", "template_action"),
        # this is primitive defined in this file, below
        "template_action": ("template_action", None, None),
        # obtain
        "obtain": ("dep_obtain", None, "locate"),
        # locate
        "locate": ("dep_locate", None, "add"),
        # add
        "add": ("dep_add", None, "dqa"),
        # dqa
        "dqa": ("dep_dqa", None, None)
    }

    def __init__(self):
        """
        Constructor
        """
        BasePipeline.__init__(self)

    def template_action(self, action, context):
        print("Template action", action)
        return None
