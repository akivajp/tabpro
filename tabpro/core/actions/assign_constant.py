from ..classes.row import Row
from .types import AssignConstantConfig

def assign_constant(
    row: Row,
    config: AssignConstantConfig,
):
    row.staging[config.target] = config.value
    return row
