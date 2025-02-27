'''
Set the value of a field in a nested dictionary.
'''

from collections import OrderedDict
from icecream import ic

def set_nested_field_value(
    data: OrderedDict,
    field: str,
    value: any,
):
    if isinstance(field, str) and  '.' in field:
        field, rest = field.split('.', 1)
        sub_data = data.get(field)
        if not isinstance(sub_data, dict):
            data[field] = OrderedDict()
        set_nested_field_value(data[field], rest, value)
    else:
        try:
            data[field] = value
        except:
            ic(data, field, value)
            raise
