from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class SEPlugin(AirflowPlugin):
    name = "se_plugin"
    operators = [
        operators.StreetEasyOperator,
        operators.ValidSearchStatsOperator
    ]
    helpers = [
    ]
