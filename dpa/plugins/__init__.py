from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin
from .operators import *


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.RedshiftStagingOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]
